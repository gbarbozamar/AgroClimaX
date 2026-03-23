import { API_BASE, API_V1, fetchCustomState, fetchHistory, fetchScopeState, fetchUnits } from './api.js?v=20260323-1';
import { initMap, setUnitsOnMap, updateFocus } from './map.js';
import { normalizeState, populateDepartmentSelect, renderChart, renderDashboard, renderDrivers, renderError, renderForecast, renderHistory, renderLoading } from './render.js';
import { setStore, store } from './state.js';

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

async function loadUnits() {
  const payload = await fetchUnits();
  setStore({ units: payload.datos || [] });
  populateDepartmentSelect(store.units);
  setUnitsOnMap(store.units, handleDepartmentSelect);
}

async function loadUnitsSafe() {
  try {
    await loadUnits();
  } catch (error) {
    console.warn('No se pudo cargar la lista de unidades al inicio:', error);
    setStore({ units: [] });
  }
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
      unit = store.units.find((item) => item.department === department || item.id === unitId) || null;
    }

    const model = normalizeState(data, {
      history: historyContextFromV1(history),
      unitLat: unit?.centroid_lat ?? null,
      unitLon: unit?.centroid_lon ?? null,
      scopeLabel: scope === 'nacional' ? 'Uruguay' : (department || unit?.name || 'Unidad'),
    });
    renderDashboard(model);
    renderDrivers(model);
    renderForecast(model);
    renderHistory(model);
    setStore({ chart: renderChart(model, store.chart), selectedScope: scope, selectedDepartment: department, selectedUnitId: unitId });
    updateFocus(model);
  } catch (error) {
    renderError(`No se pudo cargar el dashboard: ${error.message}`);
  }
}

function handleDepartmentSelect(department) {
  const select = document.getElementById('department-select');
  if (select) select.value = department;
  setStore({ customGeojson: null });
  document.getElementById('btn-limpiar').style.display = 'none';
  loadSelection('departamento', department, null);
}

async function bootstrap() {
  await initMap(async (geojson) => {
    setStore({ customGeojson: geojson });
    await loadSelection('custom');
  }, handleDepartmentSelect);

  const select = document.getElementById('department-select');
  select.addEventListener('change', async (event) => {
    const value = event.target.value;
    setStore({ customGeojson: null });
    if (value === 'nacional') {
      await loadSelection('nacional');
      return;
    }
    await loadSelection('departamento', value);
  });

  const unitsPromise = loadUnitsSafe();
  await loadSelection('nacional');
  await unitsPromise;
}

document.addEventListener('DOMContentLoaded', bootstrap);
