import {
  deleteEstablishment,
  deleteField,
  deletePaddock,
  fetchField,
  fetchFields,
  fetchFieldsGeojson,
  fetchFarmOptions,
  fetchPaddocksGeojson,
  saveEstablishment,
  saveField,
  savePaddock,
  searchPadron,
} from './api.js?v=20260420-4';
import {
  clearFarmGeometryEditor,
  fitGeojsonBounds,
  highlightFarmField,
  highlightFarmPaddock,
  setFarmFieldsOnMap,
  setFarmGuideOnMap,
  setFarmPaddocksOnMap,
  startFarmGeometryEditor,
} from './map.js?v=20260420-4';
import { setSidebarView } from './settings.js?v=20260420-4';
import { setStore, store } from './state.js?v=20260420-4';

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

function emptyFeatureCollection() {
  return { type: 'FeatureCollection', features: [], metadata: { count: 0 } };
}

function formatNumber(value, digits = 1, fallback = 'N/D') {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return fallback;
  return numeric.toFixed(digits);
}

function formatPercent(value, digits = 1) {
  return `${formatNumber(value, digits)}%`;
}

function formatIndex(value, digits = 3) {
  return formatNumber(value, digits);
}

function analyticsBadgeTone(state) {
  const tones = {
    Normal: '#2ecc71',
    Vigilancia: '#f1c40f',
    Alerta: '#e67e22',
    Emergencia: '#e74c3c',
  };
  return tones[state] || '#4a90d9';
}

function renderMetricsGrid(analytics) {
  if (!analytics) {
    return '<div class="fields-empty">Sin analitica vigente para esta geometria.</div>';
  }
  const metrics = [
    ['Estado', analytics.state || 'N/D'],
    ['Riesgo', formatNumber(analytics.risk_score, 1)],
    ['Confianza', formatPercent(analytics.confidence_score, 1)],
    ['Area afectada', formatPercent(analytics.affected_pct, 1)],
    ['Humedad S1', formatPercent(analytics.s1_humidity_mean_pct, 1)],
    ['VV dB', formatIndex(analytics.s1_vv_db_mean, 3)],
    ['NDMI', formatIndex(analytics.s2_ndmi_mean, 3)],
    ['NDMI est.', formatIndex(analytics.estimated_ndmi, 3)],
    ['SPI-30', formatIndex(analytics.spi_30d, 3)],
    ['Forecast', analytics.forecast_peak_risk != null ? formatNumber(analytics.forecast_peak_risk, 1) : 'N/D'],
    ['Driver', analytics.primary_driver || 'N/D'],
    ['Modo', analytics.analytics_mode === 'paddock_weighted' ? 'Promedio potreros' : 'Campo directo'],
  ];
  return `
    <div class="fields-metrics-grid">
      ${metrics.map(([label, value]) => `
        <div class="fields-metric">
          <span class="fields-metric-label">${escapeHtml(label)}</span>
          <span class="fields-metric-value">${escapeHtml(value)}</span>
        </div>
      `).join('')}
    </div>
  `;
}

function renderAnalyticsCard(nodeId, title, analytics, subtitle = '') {
  const node = getNode(nodeId);
  if (!node) return;
  if (!analytics) {
    node.classList.add('hidden');
    node.innerHTML = '';
    return;
  }
  const tone = analyticsBadgeTone(analytics.state);
  node.classList.remove('hidden');
  node.innerHTML = `
    <div class="fields-analytics-header">
      <div class="fields-analytics-title">
        <strong>${escapeHtml(title)}</strong>
        <span class="fields-analytics-copy">${escapeHtml(subtitle)}</span>
      </div>
      <span class="fields-inline-badge" style="border-color:${tone}; box-shadow: inset 0 0 0 1px ${tone}33;">${escapeHtml(analytics.state || 'N/D')}</span>
    </div>
    ${renderMetricsGrid(analytics)}
  `;
}

function renderCompactAnalytics(analytics) {
  if (!analytics) return '';
  return `
    <div class="fields-list-analytics">
      <span>Estado: ${escapeHtml(analytics.state || 'N/D')}</span>
      <span>Riesgo: ${escapeHtml(formatNumber(analytics.risk_score, 1))}</span>
      <span>Conf.: ${escapeHtml(formatPercent(analytics.confidence_score, 1))}</span>
      <span>Hum.: ${escapeHtml(formatPercent(analytics.s1_humidity_mean_pct, 1))}</span>
      <span>NDMI: ${escapeHtml(formatIndex(analytics.s2_ndmi_mean, 3))}</span>
      <span>SPI-30: ${escapeHtml(formatIndex(analytics.spi_30d, 3))}</span>
    </div>
  `;
}

function currentDraftGeometry(draftType) {
  if (store.farmDraftType === draftType && store.farmDraftLayer?.toGeoJSON) {
    return store.farmDraftLayer.toGeoJSON()?.geometry || null;
  }
  return draftType === 'paddock'
    ? (store.paddockDraftGeometry || selectedPaddockSummary()?.geometry_geojson || null)
    : (store.fieldDraftGeometry || store.selectedFieldDetail?.field_geometry_geojson || null);
}

function nextPaddockNameSuggestion() {
  const existing = store.selectedFieldDetail?.paddocks || [];
  return `Potrero ${existing.length + 1}`;
}

let pendingPaddockConfirmGeometry = null;

function getPaddockConfirmDialog() {
  return getNode('fields-paddock-confirm-dialog');
}

function getPaddockConfirmInput() {
  return getNode('fields-paddock-confirm-name');
}

function getPaddockConfirmStatus() {
  return getNode('fields-paddock-confirm-status');
}

function setPaddockConfirmStatus(message, tone = 'info') {
  const node = getPaddockConfirmStatus();
  if (!node) return;
  node.textContent = message;
  node.dataset.tone = tone;
}

function closePaddockConfirmDialog({ keepDraft = true } = {}) {
  const dialog = getPaddockConfirmDialog();
  if (dialog) {
    dialog.classList.add('hidden');
    dialog.setAttribute('aria-hidden', 'true');
  }
  if (!keepDraft) {
    pendingPaddockConfirmGeometry = null;
  }
}

function openPaddockConfirmDialog(geometry) {
  const dialog = getPaddockConfirmDialog();
  const input = getPaddockConfirmInput();
  if (!dialog || !input) {
    setStore({ paddockDraftGeometry: geometry });
    setStatus('Potrero cerrado. Completa el nombre y guardalo manualmente.', 'info');
    return;
  }
  pendingPaddockConfirmGeometry = geometry;
  setStore({ paddockDraftGeometry: geometry });
  input.value = nextPaddockNameSuggestion();
  input.classList.remove('invalid');
  setPaddockConfirmStatus('Completa el nombre y guarda para continuar.', 'info');
  setStatus('Potrero cerrado. Confirma el nombre para guardarlo y seguir.', 'info');
  dialog.classList.remove('hidden');
  dialog.setAttribute('aria-hidden', 'false');
  window.setTimeout(() => {
    input.focus();
    input.select();
  }, 0);
}

async function confirmPendingPaddock() {
  const input = getPaddockConfirmInput();
  const geometry = pendingPaddockConfirmGeometry;
  if (!input || !geometry) {
    closePaddockConfirmDialog({ keepDraft: false });
    return;
  }
  const finalName = input.value.trim();
  if (!finalName) {
    input.classList.add('invalid');
    setPaddockConfirmStatus('El nombre del potrero no puede quedar vacio.', 'error');
    setStatus('El nombre del potrero no puede quedar vacio.', 'error');
    input.focus();
    return;
  }
  input.classList.remove('invalid');
  const nameNode = getNode('fields-paddock-name');
  if (nameNode) {
    nameNode.value = finalName;
  }
  setPaddockConfirmStatus('Guardando potrero...', 'info');
  try {
    await persistPaddock({ nameOverride: finalName, geometryOverride: geometry, restartDrawing: true });
    closePaddockConfirmDialog({ keepDraft: false });
  } catch (error) {
    pendingPaddockConfirmGeometry = geometry;
    setStore({ paddockDraftGeometry: geometry });
    setPaddockConfirmStatus(`No se pudo guardar: ${error?.message || 'Error inesperado'}`, 'error');
    setStatus(`No se pudo guardar el potrero: ${error?.message || 'Error inesperado'}`, 'error');
    input.focus();
    input.select();
  }
}

function cancelPendingPaddock() {
  if (pendingPaddockConfirmGeometry) {
    setStore({ paddockDraftGeometry: pendingPaddockConfirmGeometry });
  }
  closePaddockConfirmDialog({ keepDraft: false });
  setStatus('Potrero cerrado pero no guardado. Puedes editar el nombre y guardarlo manualmente.', 'info');
}

function setStatus(message, tone = 'muted') {
  const node = getNode('fields-status');
  if (!node) return;
  node.textContent = message;
  node.dataset.tone = tone;
}

function selectedFieldSummary() {
  return (store.farmFields || []).find((item) => item.id === store.selectedFieldId) || null;
}

function selectedPaddockSummary() {
  return (store.selectedFieldDetail?.paddocks || []).find((item) => item.id === store.selectedPaddockId) || null;
}

function syncEstablishmentSelect() {
  const select = getNode('fields-establishment-select');
  if (!select) return;
  const items = store.farmEstablishments || [];
  select.innerHTML = [
    '<option value="">Sin establecimiento</option>',
    ...items.map((item) => `<option value="${escapeHtml(item.id)}">${escapeHtml(item.name)}</option>`),
  ].join('');
  select.value = store.selectedEstablishmentId || '';
}

function syncDepartmentSelect() {
  const select = getNode('fields-department-select');
  if (!select) return;
  const departments = store.farmOptions?.departments || [];
  select.innerHTML = [
    '<option value="">Seleccionar departamento</option>',
    ...departments.map((item) => `<option value="${escapeHtml(item.label)}">${escapeHtml(item.label)}</option>`),
  ].join('');
  const selected = store.selectedFieldDetail?.department
    || store.selectedPadronSearch?.feature?.properties?.DEPTO
    || '';
  select.value = selected;
}

function renderPadronResult() {
  const node = getNode('fields-padron-result');
  if (!node) return;
  const search = store.selectedPadronSearch;
  if (!search) {
    node.innerHTML = '<div class="fields-empty">Busca un padrón para ubicarlo como guía oficial.</div>';
    return;
  }
  if (!search.found) {
    node.innerHTML = '<div class="fields-empty">No se encontró un padrón único para esa combinación de departamento y padrón.</div>';
    return;
  }
  const props = search.feature?.properties || {};
  node.innerHTML = `
    <div class="fields-result-row"><strong>Padron</strong><span>${escapeHtml(props.PADRON || '-')}</span></div>
    <div class="fields-result-row"><strong>Departamento</strong><span>${escapeHtml(props.DEPTO || '-')}</span></div>
    <div class="fields-result-row"><strong>Area oficial</strong><span>${escapeHtml(search.area_ha || props.AREAHA || '-')} ha</span></div>
    <div class="fields-result-row"><strong>Fuente</strong><span>${escapeHtml(search.provider || 'SNIG')}</span></div>
  `;
}

function renderFieldsList() {
  const node = getNode('fields-list');
  if (!node) return;
  const items = store.farmFields || [];
  if (!items.length) {
    node.innerHTML = '<div class="fields-empty">Todavía no hay campos cargados para esta cuenta.</div>';
    return;
  }
  node.innerHTML = items.map((item) => `
    <button class="fields-list-item ${item.id === store.selectedFieldId ? 'active' : ''}" type="button" data-field-id="${escapeHtml(item.id)}">
      <span class="fields-list-title">${escapeHtml(item.name)}</span>
      <span class="fields-list-copy">${escapeHtml(item.establishment_name || 'Sin establecimiento')} · ${escapeHtml(item.department || '-')} · ${escapeHtml(item.area_ha || '-')} ha</span>
    </button>
  `).join('');
  node.querySelectorAll('[data-field-id]').forEach((button) => {
    const item = items.find((entry) => entry.id === button.dataset.fieldId);
    if (item?.field_analytics) {
      button.insertAdjacentHTML('beforeend', renderCompactAnalytics(item.field_analytics));
    }
    button.addEventListener('click', async () => {
      await selectField(button.dataset.fieldId);
    });
  });
}

function renderPaddocksList() {
  const node = getNode('fields-paddocks-list');
  if (!node) return;
  const items = store.selectedFieldDetail?.paddocks || [];
  if (!items.length) {
    node.innerHTML = '<div class="fields-empty">No hay potreros cargados para el campo seleccionado.</div>';
    return;
  }
  node.innerHTML = items.map((item) => `
    <button class="fields-list-item compact ${item.id === store.selectedPaddockId ? 'active' : ''}" type="button" data-paddock-id="${escapeHtml(item.id)}">
      <span class="fields-list-title">${escapeHtml(item.name)}</span>
      <span class="fields-list-copy">${escapeHtml(item.area_ha || '-')} ha</span>
    </button>
  `).join('');
  node.querySelectorAll('[data-paddock-id]').forEach((button) => {
    const item = items.find((entry) => entry.id === button.dataset.paddockId);
    if (item?.paddock_analytics) {
      button.insertAdjacentHTML('beforeend', renderCompactAnalytics(item.paddock_analytics));
    }
    button.addEventListener('click', () => {
      const item = (store.selectedFieldDetail?.paddocks || []).find((entry) => entry.id === button.dataset.paddockId);
      if (!item) return;
      setStore({ selectedPaddockId: item.id, paddockDraftGeometry: item.geometry_geojson });
      highlightFarmPaddock(item.id, true);
      syncFormValues();
      setStatus(`Potrero ${item.name} seleccionado.`, 'info');
    });
  });
}

function syncFormValues() {
  syncEstablishmentSelect();
  syncDepartmentSelect();
  const establishment = (store.farmEstablishments || []).find((item) => item.id === store.selectedEstablishmentId) || null;
  const field = store.selectedFieldDetail || selectedFieldSummary();
  const paddock = selectedPaddockSummary();
  const establishmentName = getNode('fields-establishment-name');
  const establishmentDescription = getNode('fields-establishment-description');
  const fieldName = getNode('fields-field-name');
  const padronInput = getNode('fields-padron-input');
  const paddockName = getNode('fields-paddock-name');
  const deleteEstablishmentButton = getNode('fields-establishment-delete-btn');
  const deleteFieldButton = getNode('fields-field-delete-btn');
  const deletePaddockButton = getNode('fields-paddock-delete-btn');

  if (establishmentName) establishmentName.value = establishment?.name || '';
  if (establishmentDescription) establishmentDescription.value = establishment?.description || '';
  if (fieldName) fieldName.value = field?.name || '';
  if (padronInput) padronInput.value = field?.padron_value || store.selectedPadronSearch?.feature?.properties?.PADRON || '';
  if (paddockName) paddockName.value = paddock?.name || '';
  if (deleteEstablishmentButton) deleteEstablishmentButton.disabled = !store.selectedEstablishmentId;
  if (deleteFieldButton) deleteFieldButton.disabled = !store.selectedFieldId;
  if (deletePaddockButton) deletePaddockButton.disabled = !store.selectedPaddockId;

  renderPadronResult();
  renderFieldsList();
  renderPaddocksList();
  renderAnalyticsCard(
    'fields-field-analytics',
    field?.name || 'Campo',
    field?.field_analytics || null,
    field?.analytics_mode === 'paddock_weighted'
      ? 'Promedio ponderado de potreros activos.'
      : 'Analitica directa del campo.',
  );
  renderAnalyticsCard(
    'fields-paddock-analytics',
    paddock?.name || 'Potrero',
    paddock?.paddock_analytics || null,
    paddock ? `${formatNumber(paddock.area_ha, 2)} ha` : '',
  );
}

async function refreshMapCollections() {
  const fieldsGeojson = await fetchFieldsGeojson(store.selectedEstablishmentId || null);
  setFarmFieldsOnMap(fieldsGeojson, async (props) => {
    if (props?.field_id) await selectField(props.field_id);
  }, store.selectedFieldId);
  if (store.selectedFieldId) {
    const paddocksGeojson = await fetchPaddocksGeojson(store.selectedFieldId);
    setFarmPaddocksOnMap(paddocksGeojson, (props) => {
      if (!props?.paddock_id) return;
      setStore({ selectedPaddockId: props.paddock_id });
      syncFormValues();
    }, store.selectedPaddockId);
  } else {
    setFarmPaddocksOnMap(emptyFeatureCollection(), null, null);
  }
}

async function selectField(fieldId) {
  closePaddockConfirmDialog({ keepDraft: false });
  clearFarmGeometryEditor();
  if (!fieldId) {
    setStore({
      selectedFieldId: null,
      selectedFieldDetail: null,
      selectedPaddockId: null,
      fieldDraftGeometry: null,
      paddockDraftGeometry: null,
    });
    clearFarmGeometryEditor();
    setFarmGuideOnMap(store.selectedPadronSearch?.feature || null);
    await refreshMapCollections();
    syncFormValues();
    return;
  }

  const detail = await fetchField(fieldId);
  // Al entrar en edición de un field, limpiamos cualquier selección administrativa
  // (sección policial / predio / hex / departamento en header) para que clicks
  // sobre polígonos administrativos del mapa no saquen al usuario del campo.
  const headerSel = document.getElementById('department-select');
  if (headerSel && headerSel.value !== 'nacional') {
    headerSel.value = 'nacional';
  }
  setStore({
    selectedFieldId: detail.id,
    selectedEstablishmentId: detail.establishment_id,
    selectedFieldDetail: detail,
    selectedPaddockId: null,
    fieldDraftGeometry: detail.field_geometry_geojson,
    paddockDraftGeometry: null,
    selectedDepartment: null,
    selectedUnitId: null,
    selectedSectionId: null,
    selectedProductiveId: null,
    selectedHexId: null,
  });
  setFarmGuideOnMap(detail.padron_geometry_geojson ? { type: 'Feature', geometry: detail.padron_geometry_geojson, properties: {} } : null);
  await refreshMapCollections();
  highlightFarmField(detail.id, true);
  syncFormValues();
}

async function refreshFieldsState({ preserveSelection = true } = {}) {
  const options = await fetchFarmOptions();
  const fieldsPayload = await fetchFields(store.selectedEstablishmentId || null);
  const establishments = options.establishments || [];
  const fields = fieldsPayload.items || [];
  const selectedEstablishmentId = preserveSelection && establishments.some((item) => item.id === store.selectedEstablishmentId)
    ? store.selectedEstablishmentId
    : (establishments[0]?.id || null);
  const selectedFieldId = preserveSelection && fields.some((item) => item.id === store.selectedFieldId)
    ? store.selectedFieldId
    : null;

  setStore({
    farmOptions: options,
    farmEstablishments: establishments,
    farmFields: fields,
    selectedEstablishmentId,
    selectedFieldId,
  });

  if (selectedFieldId) {
    await selectField(selectedFieldId);
  } else {
    setStore({ selectedFieldDetail: null, selectedPaddockId: null, paddockDraftGeometry: null });
    await refreshMapCollections();
    syncFormValues();
  }
}

async function persistEstablishment() {
  const name = getNode('fields-establishment-name')?.value?.trim() || '';
  const description = getNode('fields-establishment-description')?.value?.trim() || '';
  if (!name) {
    setStatus('El establecimiento debe tener nombre.', 'error');
    return;
  }
  const saved = await saveEstablishment({ name, description }, store.selectedEstablishmentId || null);
  setStore({ selectedEstablishmentId: saved.id });
  await refreshFieldsState();
  setStatus(`Establecimiento ${saved.name} guardado.`, 'success');
}

async function removeEstablishment() {
  if (!store.selectedEstablishmentId) return;
  if (!window.confirm('Se va a desactivar el establecimiento y sus campos asociados.')) return;
  await deleteEstablishment(store.selectedEstablishmentId);
  clearFarmGeometryEditor();
  setStore({
    selectedEstablishmentId: null,
    selectedFieldId: null,
    selectedFieldDetail: null,
    selectedPaddockId: null,
    fieldDraftGeometry: null,
    paddockDraftGeometry: null,
    selectedPadronSearch: null,
  });
  setFarmGuideOnMap(null);
  await refreshFieldsState({ preserveSelection: false });
  setStatus('Establecimiento eliminado.', 'success');
}

async function persistField() {
  const establishmentId = getNode('fields-establishment-select')?.value || store.selectedEstablishmentId;
  const name = getNode('fields-field-name')?.value?.trim() || '';
  const department = getNode('fields-department-select')?.value?.trim() || '';
  const padron = getNode('fields-padron-input')?.value?.trim() || '';
  const geometry = currentDraftGeometry('field');
  if (!establishmentId) {
    setStatus('Selecciona o crea un establecimiento antes de guardar un campo.', 'error');
    return;
  }
  if (!name) {
    setStatus('El campo debe tener nombre.', 'error');
    return;
  }
  if (!department || !padron) {
    setStatus('Departamento y padrón son obligatorios.', 'error');
    return;
  }
  if (!geometry) {
    setStatus('Dibuja o edita la geometría del campo antes de guardar.', 'error');
    return;
  }
  const padronFeature = store.selectedPadronSearch?.feature || null;
  const payload = {
    establishment_id: establishmentId,
    name,
    department,
    padron_value: padron,
    padron_source: store.selectedPadronSearch?.provider || store.selectedFieldDetail?.padron_source || 'snig_padronario_rural',
    padron_lookup_payload: store.selectedPadronSearch?.raw_provider || store.selectedFieldDetail?.padron_lookup_payload || {},
    padron_geometry_geojson: padronFeature?.geometry || store.selectedFieldDetail?.padron_geometry_geojson || null,
    field_geometry_geojson: geometry,
  };
  const saved = await saveField(payload, store.selectedFieldId || null);
  setStore({
    selectedEstablishmentId: saved.establishment_id,
    selectedFieldId: saved.id,
    fieldDraftGeometry: saved.field_geometry_geojson,
  });
  await refreshFieldsState();
  await selectField(saved.id);
  setStatus(`Campo ${saved.name} guardado.`, 'success');
}

async function removeField() {
  if (!store.selectedFieldId) return;
  if (!window.confirm('Se va a desactivar el campo seleccionado.')) return;
  await deleteField(store.selectedFieldId);
  clearFarmGeometryEditor();
  setStore({
    selectedFieldId: null,
    selectedFieldDetail: null,
    selectedPaddockId: null,
    fieldDraftGeometry: null,
    paddockDraftGeometry: null,
  });
  await refreshFieldsState({ preserveSelection: false });
  setStatus('Campo eliminado.', 'success');
}

async function persistPaddock({ nameOverride = null, geometryOverride = null, restartDrawing = false } = {}) {
  if (!store.selectedFieldId) {
    setStatus('Selecciona primero un campo.', 'error');
    return;
  }
  const name = (nameOverride ?? getNode('fields-paddock-name')?.value ?? '').trim();
  const geometry = geometryOverride || currentDraftGeometry('paddock');
  if (!name) {
    setStatus('El potrero debe tener nombre.', 'error');
    return;
  }
  if (!geometry) {
    setStatus('Dibuja o edita la geometría del potrero antes de guardar.', 'error');
    return;
  }
  let saved;
  try {
    saved = await savePaddock(
      store.selectedFieldId,
      { name, geometry_geojson: geometry },
      store.selectedPaddockId || null,
    );
  } catch (error) {
    setStatus(`No se pudo guardar el potrero: ${error?.message || 'Error inesperado'}`, 'error');
    throw error;
  }
  setStore({ selectedPaddockId: saved.id, paddockDraftGeometry: saved.geometry_geojson });
  await selectField(store.selectedFieldId);
  highlightFarmPaddock(saved.id, true);
  if (getNode('fields-paddock-name')) {
    getNode('fields-paddock-name').value = '';
  }
  if (restartDrawing) {
    setStore({ selectedPaddockId: null, paddockDraftGeometry: null });
    clearFarmGeometryEditor();
    beginPaddockEditingFlow();
    setStatus(`Potrero ${saved.name} guardado. Sigue dibujando el siguiente.`, 'success');
    return;
  }
  syncFormValues();
  setStatus(`Potrero ${saved.name} guardado.`, 'success');
}

async function removePaddock() {
  if (!store.selectedFieldId || !store.selectedPaddockId) return;
  if (!window.confirm('Se va a desactivar el potrero seleccionado.')) return;
  await deletePaddock(store.selectedFieldId, store.selectedPaddockId);
  clearFarmGeometryEditor();
  setStore({ selectedPaddockId: null, paddockDraftGeometry: null });
  await selectField(store.selectedFieldId);
  setStatus('Potrero eliminado.', 'success');
}

async function handlePadronSearch() {
  const department = getNode('fields-department-select')?.value?.trim() || '';
  const padron = getNode('fields-padron-input')?.value?.trim() || '';
  if (!department || !padron) {
    setStatus('Selecciona departamento y completa el padrón para buscar.', 'error');
    return;
  }
  setStatus('Buscando padrón oficial...', 'info');
  const result = await searchPadron(department, padron);
  setStore({ selectedPadronSearch: result });
  renderPadronResult();
  if (result.found && result.feature) {
    // Entramos en contexto farm: desconfiguramos zoom/selección de país/departamento para que clicks
    // accidentales en polígonos administrativos no saquen al usuario del padrón.
    const headerSel = document.getElementById('department-select');
    if (headerSel && headerSel.value !== 'nacional') {
      headerSel.value = 'nacional';
    }
    setStore({
      selectedDepartment: null,
      selectedUnitId: null,
      selectedSectionId: null,
      selectedProductiveId: null,
      selectedHexId: null,
    });
    setFarmGuideOnMap(result.feature);
    if (!store.selectedFieldId) {
      setStore({ fieldDraftGeometry: result.feature.geometry });
    }
    setStatus('Padrón encontrado. Ya puedes ajustar el contorno real del campo.', 'success');
    return;
  }
  setFarmGuideOnMap(null);
  setStatus('No se encontró un padrón único para esa búsqueda.', 'error');
}

function beginFieldEditing() {
  const geometry = store.fieldDraftGeometry || store.selectedFieldDetail?.field_geometry_geojson || store.selectedPadronSearch?.feature?.geometry || null;
  const started = startFarmGeometryEditor({
    mode: 'field',
    geometry,
    onChange: (nextGeometry) => setStore({ fieldDraftGeometry: nextGeometry }),
  });
  if (!started) {
    setStatus('Leaflet-Geoman no está disponible en esta carga.', 'error');
    return;
  }
  setStatus('Editor de campo activo. Ajusta el contorno y luego guarda.', 'info');
}

function beginPaddockEditing() {
  if (!store.selectedFieldId) {
    setStatus('Selecciona un campo antes de dibujar potreros.', 'error');
    return;
  }
  const geometry = store.paddockDraftGeometry || selectedPaddockSummary()?.geometry_geojson || null;
  const started = startFarmGeometryEditor({
    mode: 'paddock',
    geometry,
    onChange: (nextGeometry) => setStore({ paddockDraftGeometry: nextGeometry }),
  });
  if (!started) {
    setStatus('Leaflet-Geoman no está disponible en esta carga.', 'error');
    return;
  }
  setStatus('Editor de potrero activo. Dibuja o ajusta el polígono y luego guarda.', 'info');
}

function beginPaddockEditingFlow() {
  if (!store.selectedFieldId) {
    setStatus('Selecciona un campo antes de dibujar potreros.', 'error');
    return;
  }
  const geometry = store.paddockDraftGeometry || selectedPaddockSummary()?.geometry_geojson || null;
  const isNewPaddock = !store.selectedPaddockId;
  const started = startFarmGeometryEditor({
    mode: 'paddock',
    geometry,
    onChange: (nextGeometry) => {
      setStore({ paddockDraftGeometry: nextGeometry });
      if (isNewPaddock && nextGeometry && !pendingPaddockConfirmGeometry) {
        openPaddockConfirmDialog(nextGeometry);
      }
    },
    onComplete: null,
  });
  if (!started) {
    setStatus('Leaflet-Geoman no esta disponible en esta carga.', 'error');
    return;
  }
  setStatus(
    isNewPaddock
      ? 'Editor de potrero activo. Cierra el poligono para confirmar nombre y seguir con el siguiente.'
      : 'Editor de potrero activo. Ajusta el poligono y luego guarda.',
    'info',
  );
}

function resetFieldDraft() {
  clearFarmGeometryEditor();
  setStore({
    fieldDraftGeometry: store.selectedFieldDetail?.field_geometry_geojson || null,
    paddockDraftGeometry: null,
    selectedPaddockId: null,
  });
  if (store.selectedFieldDetail?.field_geometry_geojson) {
    fitGeojsonBounds(store.selectedFieldDetail.field_geometry_geojson, 16);
  }
  setStatus('Borrador de campo reiniciado.', 'info');
}

function resetPaddockDraft() {
  closePaddockConfirmDialog({ keepDraft: false });
  clearFarmGeometryEditor();
  const paddock = selectedPaddockSummary();
  setStore({ paddockDraftGeometry: paddock?.geometry_geojson || null });
  if (paddock?.geometry_geojson) {
    fitGeojsonBounds(paddock.geometry_geojson, 16);
  }
  setStatus('Borrador de potrero reiniciado.', 'info');
}

function bindEvents() {
  getNode('sidebar-fields-tab')?.addEventListener('click', async () => {
    setSidebarView('fields');
    if (!store.farmOptions) {
      await refreshFieldsState({ preserveSelection: false });
      setStatus('Módulo Campos cargado.', 'success');
    }
  });

  getNode('fields-refresh-btn')?.addEventListener('click', async () => {
    await refreshFieldsState();
    setStatus('Campos y establecimientos recargados.', 'success');
  });

  getNode('fields-establishment-select')?.addEventListener('change', async (event) => {
    clearFarmGeometryEditor();
    setStore({
      selectedEstablishmentId: event.target.value || null,
      selectedFieldId: null,
      selectedFieldDetail: null,
      selectedPaddockId: null,
      fieldDraftGeometry: null,
      paddockDraftGeometry: null,
    });
    await refreshFieldsState({ preserveSelection: false });
  });

  getNode('fields-establishment-save-btn')?.addEventListener('click', async () => {
    await persistEstablishment();
  });
  getNode('fields-establishment-delete-btn')?.addEventListener('click', async () => {
    await removeEstablishment();
  });
  getNode('fields-padron-search-btn')?.addEventListener('click', async () => {
    await handlePadronSearch();
  });
  getNode('fields-field-draw-btn')?.addEventListener('click', beginFieldEditing);
  getNode('fields-field-save-btn')?.addEventListener('click', async () => {
    await persistField();
  });
  getNode('fields-field-delete-btn')?.addEventListener('click', async () => {
    await removeField();
  });
  getNode('fields-field-clear-btn')?.addEventListener('click', resetFieldDraft);
  getNode('fields-paddock-draw-btn')?.addEventListener('click', beginPaddockEditingFlow);
  getNode('fields-paddock-save-btn')?.addEventListener('click', async () => {
    await persistPaddock();
  });
  getNode('fields-paddock-delete-btn')?.addEventListener('click', async () => {
    await removePaddock();
  });
  getNode('fields-paddock-clear-btn')?.addEventListener('click', resetPaddockDraft);
  getNode('fields-paddock-confirm-save-btn')?.addEventListener('click', async () => {
    await confirmPendingPaddock();
  });
  getNode('fields-paddock-confirm-cancel-btn')?.addEventListener('click', () => {
    cancelPendingPaddock();
  });
  getPaddockConfirmDialog()?.addEventListener('click', (event) => {
    if (event.target === getPaddockConfirmDialog()) {
      cancelPendingPaddock();
    }
  });
  getPaddockConfirmInput()?.addEventListener('input', () => {
    getPaddockConfirmInput()?.classList.remove('invalid');
  });
  getPaddockConfirmInput()?.addEventListener('keydown', async (event) => {
    if (event.key !== 'Enter') return;
    event.preventDefault();
    await confirmPendingPaddock();
  });
}

export async function initFieldsPanel() {
  bindEvents();
}
