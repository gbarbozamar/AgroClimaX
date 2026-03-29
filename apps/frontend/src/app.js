import { API_BASE, API_V1, downloadJsonFile, fetchCustomState, fetchDepartmentLayers, fetchHexagonsGeojson, fetchHistory, fetchProductiveTemplate, fetchProductiveUnits, fetchProductiveUnitsGeojson, fetchScopeState, fetchSectionsGeojson, fetchUnits, fetchWeatherForecast, uploadProductiveUnitsFile } from './api.js?v=20260329-7';
import { initAuth } from './auth.js?v=20260329-7';
import { highlightDepartment, highlightHex, highlightProductive, highlightSection, initMap, setDepartmentsOnMap, setHexesOnMap, setProductivesOnMap, setSectionsOnMap, updateFocus } from './map.js?v=20260329-2';
import { initProfilePanel, refreshProfilePanel } from './profile.js?v=20260329-6';
import { normalizeState, populateDepartmentSelect, renderChart, renderDashboard, renderDrivers, renderError, renderForecast, renderHistory, renderLoading, renderWeatherCards } from './render.js?v=20260327-15';
import { initSettingsPanel } from './settings.js?v=20260329-5';
import { setStore, store } from './state.js?v=20260329-2';

setStore({ apiBase: API_BASE, apiV1: API_V1 });

function historyContextFromV1(history) {
  return (history?.datos || []).map((item) => ({
    fecha: item.fecha,
    state: item.state,
    state_level: item.state_level,
    risk_score: item.risk_score,
    affected_pct: item.affected_pct,
  }));
}

function currentDepartmentFilter() {
  const select = document.getElementById('department-select');
  if (!select || select.value === 'nacional') return null;
  return select.value;
}

function selectedSectionProps(unitId) {
  return unitId ? store.sectionsLookup?.[unitId]?.feature?.properties || null : null;
}

function selectedProductiveProps(unitId) {
  return unitId ? store.productiveLookup?.[unitId]?.feature?.properties || null : null;
}

function selectedHexProps(unitId) {
  return unitId ? store.hexLookup?.[unitId]?.feature?.properties || null : null;
}

function currentSelectionWeatherOption() {
  if (store.customGeojson) {
    return { value: 'current', label: 'Actual: parcela custom', mode: 'current' };
  }
  if (store.selectedProductiveId) {
    const props = selectedProductiveProps(store.selectedProductiveId);
    return {
      value: 'current',
      label: `Actual: ${props?.unit_name || 'predio'}`,
      mode: 'current',
      scope: 'unidad',
      unitId: store.selectedProductiveId,
    };
  }
  if (store.selectedSectionId) {
    const props = selectedSectionProps(store.selectedSectionId);
    return {
      value: 'current',
      label: `Actual: ${props?.unit_name || 'jurisdiccion'}`,
      mode: 'current',
      scope: 'unidad',
      unitId: store.selectedSectionId,
    };
  }
  if (store.selectedHexId) {
    return { value: 'current', label: 'Actual: hexagono', mode: 'current', scope: 'unidad', unitId: store.selectedHexId };
  }
  if (store.selectedScope === 'departamento' && store.selectedDepartment) {
    return { value: 'current', label: `Actual: ${store.selectedDepartment}`, mode: 'current', scope: 'departamento', department: store.selectedDepartment };
  }
  return { value: 'current', label: 'Actual: Uruguay', mode: 'current', scope: 'nacional' };
}

function buildWeatherFilterOptions() {
  const options = [currentSelectionWeatherOption()];
  options.push({ value: 'scope:nacional', label: 'Uruguay', scope: 'nacional' });

  store.units.forEach((unit) => {
    options.push({
      value: `scope:departamento:${unit.department}`,
      label: `Depto: ${unit.department}`,
      scope: 'departamento',
      department: unit.department,
    });
  });

  if (store.currentLayer === 'judicial' && store.sectionsLookup) {
    Object.values(store.sectionsLookup).forEach((layer) => {
      const props = layer?.feature?.properties || {};
      if (!props.unit_id) return;
      options.push({
        value: `scope:unidad:${props.unit_id}`,
        label: `Jurisdiccion: ${props.unit_name || props.unit_id}`,
        scope: 'unidad',
        unitId: props.unit_id,
      });
    });
  }

  if (store.selectedProductiveId) {
    const props = selectedProductiveProps(store.selectedProductiveId);
    options.push({
      value: `scope:unidad:${store.selectedProductiveId}`,
      label: `Predio: ${props?.unit_name || store.selectedProductiveId}`,
      scope: 'unidad',
      unitId: store.selectedProductiveId,
    });
  }

  if (store.selectedHexId) {
    options.push({
      value: `scope:unidad:${store.selectedHexId}`,
      label: `H3: ${store.selectedHexId}`,
      scope: 'unidad',
      unitId: store.selectedHexId,
    });
  }

  const unique = new Map();
  options.forEach((option) => {
    if (!unique.has(option.value)) unique.set(option.value, option);
  });
  return Array.from(unique.values());
}

function syncWeatherFilterOptions() {
  const select = document.getElementById('weather-filter-select');
  if (!select) return;
  const options = buildWeatherFilterOptions();
  setStore({ weatherFilterOptions: options });
  const currentValue = options.some((option) => option.value === store.weatherFilterValue)
    ? store.weatherFilterValue
    : 'current';
  select.innerHTML = '';
  options.forEach((option) => {
    const node = document.createElement('option');
    node.value = option.value;
    node.textContent = option.label;
    if (option.value === currentValue) node.selected = true;
    select.appendChild(node);
  });
  setStore({ weatherFilterValue: currentValue });
}

async function refreshWeatherCards() {
  const options = store.weatherFilterOptions || [];
  const activeOption = options.find((option) => option.value === store.weatherFilterValue) || currentSelectionWeatherOption();

  if (activeOption.mode === 'current' && !activeOption.scope) {
    renderWeatherCards(store.currentModel, activeOption.label);
    return;
  }

  try {
    const weatherPayload = await fetchWeatherForecast(activeOption.scope, activeOption.department || null, activeOption.unitId || null);
    setStore({ weatherModel: weatherPayload });
    renderWeatherCards(weatherPayload, activeOption.label || weatherPayload.selection_label || 'Seleccion actual');
  } catch (error) {
    console.warn('No se pudo refrescar el bloque meteorologico:', error);
    renderWeatherCards(store.currentModel, currentSelectionWeatherOption().label);
  }
}

function setProductiveImportStatus(message, tone = 'muted') {
  const node = document.getElementById('productive-import-status');
  if (!node) return;
  node.textContent = message;
  node.style.color = tone === 'error'
    ? '#e74c3c'
    : tone === 'success'
      ? '#2ecc71'
      : tone === 'info'
        ? '#4a90d9'
        : 'var(--text-muted)';
}

async function refreshProductiveImportSummary(department = null) {
  const countNode = document.getElementById('productive-import-count');
  if (!countNode) return;
  try {
    const payload = await fetchProductiveUnits(department);
    const total = payload?.total || 0;
    countNode.textContent = String(total);
  } catch (error) {
    console.warn('No se pudo actualizar el resumen de productivas:', error);
  }
}

async function loadUnits() {
  const payload = await fetchUnits();
  setStore({ units: payload.datos || [] });
  populateDepartmentSelect(store.units);
}

async function loadUnitsSafe() {
  try {
    await loadUnits();
  } catch (error) {
    console.warn('No se pudo cargar la lista de unidades al inicio:', error);
    setStore({ units: [] });
  }
}

async function loadSectionsLayer(department = null) {
  const loading = document.getElementById('map-tile-loading');
  try {
    if (loading) {
      loading.textContent = department ? `Cargando secciones de ${department}...` : 'Cargando secciones policiales...';
      loading.style.display = 'block';
    }
    const collection = await fetchSectionsGeojson(department);
    setSectionsOnMap(collection, handleSectionSelect, store.selectedSectionId);
    syncWeatherFilterOptions();
  } catch (error) {
    console.warn('No se pudo cargar la capa de secciones:', error);
  } finally {
    if (loading) loading.style.display = 'none';
  }
}

async function loadDepartmentLayer(selectedDepartment = null) {
  const loading = document.getElementById('map-tile-loading');
  try {
    if (loading) {
      loading.textContent = 'Cargando capa departamental...';
      loading.style.display = 'block';
    }
    const collection = await fetchDepartmentLayers();
    setDepartmentsOnMap(collection, handleDepartmentSelect, selectedDepartment);
    if (selectedDepartment) highlightDepartment(selectedDepartment, false);
    syncWeatherFilterOptions();
  } catch (error) {
    console.warn('No se pudo cargar la capa de departamentos:', error);
  } finally {
    if (loading) loading.style.display = 'none';
  }
}

async function loadHexLayer(department = null) {
  const loading = document.getElementById('map-tile-loading');
  try {
    if (loading) {
      loading.textContent = department ? `Cargando malla H3 de ${department}...` : 'Cargando malla H3 nacional...';
      loading.style.display = 'block';
    }
    const collection = await fetchHexagonsGeojson(department);
    setHexesOnMap(collection, handleHexSelect, store.selectedHexId);
    syncWeatherFilterOptions();
  } catch (error) {
    console.warn('No se pudo cargar la capa H3:', error);
  } finally {
    if (loading) loading.style.display = 'none';
  }
}

async function loadProductiveLayer(department = null) {
  const loading = document.getElementById('map-tile-loading');
  try {
    if (loading) {
      loading.textContent = department ? `Cargando predios de ${department}...` : 'Cargando unidades productivas...';
      loading.style.display = 'block';
    }
    const collection = await fetchProductiveUnitsGeojson(department);
    setProductivesOnMap(collection, handleProductiveSelect, store.selectedProductiveId);
    const count = collection?.metadata?.count || 0;
    const countNode = document.getElementById('productive-import-count');
    if (countNode) countNode.textContent = String(count);
    if (count === 0 && loading) {
      loading.textContent = 'No hay predios/potreros importados todavia.';
      window.setTimeout(() => {
        if (loading.textContent === 'No hay predios/potreros importados todavia.') loading.style.display = 'none';
      }, 2400);
    }
    syncWeatherFilterOptions();
  } catch (error) {
    console.warn('No se pudo cargar la capa productiva:', error);
  } finally {
    if (loading && loading.textContent !== 'No hay predios/potreros importados todavia.') loading.style.display = 'none';
  }
}

async function downloadProductiveTemplateFile() {
  setProductiveImportStatus('Descargando plantilla GeoJSON...', 'info');
  try {
    const payload = await fetchProductiveTemplate();
    downloadJsonFile('agroclimax_plantilla_productivas.geojson', payload);
    setProductiveImportStatus('Plantilla descargada. Podes completarla y volver a subirla.', 'success');
  } catch (error) {
    setProductiveImportStatus(`No se pudo descargar la plantilla: ${error.message}`, 'error');
  }
}

async function handleProductiveFileUpload() {
  const fileInput = document.getElementById('productive-file');
  const categorySelect = document.getElementById('productive-category');
  const uploadButton = document.getElementById('productive-upload-btn');
  if (!fileInput?.files?.length) {
    setProductiveImportStatus('Selecciona un archivo .geojson, .json o .zip.', 'error');
    return;
  }
  const file = fileInput.files[0];
  const category = categorySelect?.value || 'predio';
  const sourceName = `ui_${category}_${new Date().toISOString().slice(0, 10)}`;

  if (uploadButton) uploadButton.disabled = true;
  setProductiveImportStatus(`Importando ${file.name}...`, 'info');
  try {
    const result = await uploadProductiveUnitsFile(file, { category, sourceName });
    const summary = `${result.created} nuevas, ${result.updated} actualizadas, ${result.skipped} omitidas`;
    setProductiveImportStatus(`Importacion lista: ${summary}.`, 'success');
    fileInput.value = '';
    await refreshProductiveImportSummary(currentDepartmentFilter());
    const btn = document.getElementById('btn-productiva');
    if (window.setLayer) {
      await window.setLayer('productiva', btn);
    }
  } catch (error) {
    setProductiveImportStatus(`No se pudo importar el archivo: ${error.message}`, 'error');
  } finally {
    if (uploadButton) uploadButton.disabled = false;
  }
}

function wireProductiveImportControls() {
  const uploadButton = document.getElementById('productive-upload-btn');
  const templateButton = document.getElementById('productive-template-btn');
  const fileInput = document.getElementById('productive-file');

  uploadButton?.addEventListener('click', handleProductiveFileUpload);
  templateButton?.addEventListener('click', downloadProductiveTemplateFile);
  fileInput?.addEventListener('change', () => {
    if (!fileInput.files?.length) {
      setProductiveImportStatus('Sin archivo seleccionado.', 'muted');
      return;
    }
    setProductiveImportStatus(`Archivo listo: ${fileInput.files[0].name}`, 'info');
  });
}

async function loadSelection(scope, department = null, unitId = null) {
  renderLoading(scope === 'nacional' ? 'Cargando panorama nacional...' : `Cargando ${department || 'unidad'}...`);
  try {
    let data;
    let history;
    let unit = null;
    if (scope === 'custom' && store.customGeojson) {
      data = await fetchCustomState(store.customGeojson);
      history = { datos: [] };
    } else {
      data = await fetchScopeState(scope, department, unitId);
      history = await fetchHistory(scope, department, unitId, 30);
      unit = store.units.find((item) => item.department === department || item.id === unitId) || selectedSectionProps(unitId) || selectedProductiveProps(unitId) || selectedHexProps(unitId) || null;
    }

    const model = normalizeState(data, {
      history: historyContextFromV1(history),
      unitLat: unit?.centroid_lat ?? null,
      unitLon: unit?.centroid_lon ?? null,
      scopeLabel: scope === 'nacional' ? 'Uruguay' : (department || unit?.unit_name || unit?.name || 'Unidad'),
    });
    renderDashboard(model);
    renderDrivers(model);
    renderForecast(model);
    renderHistory(model);
    setStore({
      chart: renderChart(model, store.chart),
      currentModel: model,
      selectedScope: scope,
      selectedDepartment: department,
      selectedUnitId: unitId,
    });
    syncWeatherFilterOptions();
    await refreshWeatherCards();
    updateFocus(model);
    if (unitId && store.currentLayer === 'judicial') highlightSection(unitId, false);
    if (unitId && store.currentLayer === 'productiva') highlightProductive(unitId, false);
    if (unitId && store.currentLayer === 'hex') highlightHex(unitId, false);
    if (department && store.currentLayer !== 'judicial') highlightDepartment(department, false);
  } catch (error) {
    renderError(`No se pudo cargar el dashboard: ${error.message}`);
  }
}

function handleDepartmentSelect(department) {
  const select = document.getElementById('department-select');
  if (select) select.value = department;
  setStore({ customGeojson: null, selectedSectionId: null, selectedHexId: null });
  setStore({ selectedProductiveId: null });
  document.getElementById('btn-limpiar').style.display = 'none';
  refreshProductiveImportSummary(department);
  loadSelection('departamento', department, null);
  if (store.currentLayer === 'judicial') {
    loadSectionsLayer(department);
    return;
  }
  if (store.currentLayer === 'productiva') {
    loadProductiveLayer(department);
    return;
  }
  loadDepartmentLayer(department);
}

function handleSectionSelect(section) {
  setStore({ customGeojson: null, selectedSectionId: section.unit_id, selectedProductiveId: null, selectedHexId: null });
  loadSelection('unidad', null, section.unit_id);
}

function handleProductiveSelect(unit) {
  setStore({ customGeojson: null, selectedProductiveId: unit.unit_id, selectedSectionId: null, selectedHexId: null });
  loadSelection('unidad', null, unit.unit_id);
}

function handleHexSelect(hex) {
  setStore({ customGeojson: null, selectedHexId: hex.unit_id, selectedProductiveId: null, selectedSectionId: null });
  loadSelection('unidad', null, hex.unit_id);
}

async function refreshCurrentSelection() {
  if (store.customGeojson) {
    await loadSelection('custom');
    return;
  }
  if (store.selectedProductiveId) {
    await loadSelection('unidad', null, store.selectedProductiveId);
    return;
  }
  if (store.selectedSectionId) {
    await loadSelection('unidad', null, store.selectedSectionId);
    return;
  }
  if (store.selectedHexId) {
    await loadSelection('unidad', null, store.selectedHexId);
    return;
  }
  if (store.selectedScope === 'departamento' && store.selectedDepartment) {
    await loadSelection('departamento', store.selectedDepartment);
    return;
  }
  if (store.selectedScope === 'unidad' && store.selectedUnitId) {
    await loadSelection('unidad', null, store.selectedUnitId);
    return;
  }
  await loadSelection('nacional');
}

async function refreshCurrentLayer() {
  const department = currentDepartmentFilter();
  if (store.currentLayer === 'judicial') {
    await loadSectionsLayer(department);
    return;
  }
  if (store.currentLayer === 'productiva') {
    await loadProductiveLayer(department);
    return;
  }
  if (store.currentLayer === 'hex') {
    await loadHexLayer(department);
    return;
  }
  if (store.currentLayer === 'coneat') {
    return;
  }
  await loadDepartmentLayer(store.selectedDepartment);
}

async function bootstrap() {
  const authenticated = await initAuth();
  if (!authenticated) return;

  await initMap(async (geojson) => {
    setStore({ customGeojson: geojson, selectedSectionId: null, selectedProductiveId: null, selectedHexId: null });
    await loadSelection('custom');
  }, handleDepartmentSelect, handleSectionSelect);

  const originalSetLayer = window.setLayer;
  window.setLayer = async (name, btn) => {
    originalSetLayer(name, btn);
    if (name === 'judicial') {
      await loadSectionsLayer(currentDepartmentFilter());
      syncWeatherFilterOptions();
      return;
    }
    if (name === 'productiva') {
      await loadProductiveLayer(currentDepartmentFilter());
      syncWeatherFilterOptions();
      return;
    }
    if (name === 'hex') {
      await loadHexLayer(currentDepartmentFilter());
      syncWeatherFilterOptions();
      return;
    }
    if (name === 'coneat') {
      syncWeatherFilterOptions();
      return;
    }
    await loadDepartmentLayer(store.selectedDepartment);
    syncWeatherFilterOptions();
  };

  const select = document.getElementById('department-select');
  const weatherSelect = document.getElementById('weather-filter-select');
  select.addEventListener('change', async (event) => {
    const value = event.target.value;
    setStore({ customGeojson: null, selectedSectionId: null, selectedProductiveId: null, selectedHexId: null });
    if (value === 'nacional') {
      await refreshProductiveImportSummary(null);
      await loadSelection('nacional');
      if (store.currentLayer === 'judicial') {
        await loadSectionsLayer(null);
      } else if (store.currentLayer === 'productiva') {
        await loadProductiveLayer(null);
      } else if (store.currentLayer === 'hex') {
        await loadHexLayer(null);
      } else if (store.currentLayer === 'coneat') {
        return;
      } else {
        await loadDepartmentLayer(null);
      }
      return;
    }
    await refreshProductiveImportSummary(value);
    await loadSelection('departamento', value);
    if (store.currentLayer === 'judicial') {
      await loadSectionsLayer(value);
    } else if (store.currentLayer === 'productiva') {
      await loadProductiveLayer(value);
    } else if (store.currentLayer === 'hex') {
      await loadHexLayer(value);
    } else if (store.currentLayer === 'coneat') {
      return;
    } else {
      await loadDepartmentLayer(value);
    }
    syncWeatherFilterOptions();
  });

  weatherSelect?.addEventListener('change', async (event) => {
    setStore({ weatherFilterValue: event.target.value });
    await refreshWeatherCards();
  });

  const unitsPromise = loadUnitsSafe();
  const departmentsPromise = loadDepartmentLayer(null);
  await loadSelection('nacional');
  await unitsPromise;
  await departmentsPromise;
  syncWeatherFilterOptions();
  await refreshWeatherCards();
  await refreshProductiveImportSummary(null);
  wireProductiveImportControls();
  initSettingsPanel({
    onRefreshSelection: refreshCurrentSelection,
    onRefreshLayers: refreshCurrentLayer,
  });
  initProfilePanel();
  await refreshProfilePanel();
  setProductiveImportStatus('Subi un .geojson o .zip shapefile para activar la capa Predios.', 'muted');
}

document.addEventListener('DOMContentLoaded', bootstrap);
