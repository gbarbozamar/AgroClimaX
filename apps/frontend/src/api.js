const params = new URLSearchParams(window.location.search);
const isHttpOrigin = window.location.protocol === 'http:' || window.location.protocol === 'https:';
const defaultApiBase = isHttpOrigin ? '/api' : 'http://localhost:8000/api';

export const API_BASE =
  params.get('api') ||
  defaultApiBase;

export const API_V1 = `${API_BASE}/v1`;

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const raw = await response.text();
  let data;
  try {
    data = raw ? JSON.parse(raw) : {};
  } catch {
    data = { detail: raw || `HTTP ${response.status}` };
  }
  if (!response.ok) {
    throw new Error(data.detail || data.error || `HTTP ${response.status}`);
  }
  return data;
}

export async function fetchUnits() {
  return fetchJson(`${API_V1}/unidades`);
}

export async function fetchDepartmentLayers(department = null) {
  const url = new URL(`${API_V1}/capas/departamentos`, window.location.origin);
  if (department) url.searchParams.set('department', department);
  return fetchJson(url.toString().replace(window.location.origin, ''));
}

export async function fetchSectionsGeojson(department = null) {
  const url = new URL(`${API_V1}/capas/secciones`, window.location.origin);
  if (department) url.searchParams.set('department', department);
  return fetchJson(url.toString().replace(window.location.origin, ''));
}

export async function fetchHexagonsGeojson(department = null) {
  const url = new URL(`${API_V1}/capas/hexagonos`, window.location.origin);
  if (department) url.searchParams.set('department', department);
  return fetchJson(url.toString().replace(window.location.origin, ''));
}

export async function fetchProductiveUnitsGeojson(department = null) {
  const url = new URL(`${API_V1}/capas/productivas`, window.location.origin);
  if (department) url.searchParams.set('department', department);
  return fetchJson(url.toString().replace(window.location.origin, ''));
}

export async function fetchProductiveUnits(department = null) {
  const url = new URL(`${API_V1}/productivas`, window.location.origin);
  if (department) url.searchParams.set('department', department);
  return fetchJson(url.toString().replace(window.location.origin, ''));
}

export async function fetchProductiveTemplate() {
  return fetchJson(`${API_V1}/productivas/plantilla`);
}

export async function uploadProductiveUnitsFile(file, { category = 'predio', sourceName = 'ui_upload' } = {}) {
  const formData = new FormData();
  formData.append('file', file);
  formData.append('category', category);
  formData.append('source_name', sourceName);
  const response = await fetch(`${API_V1}/productivas/import-archivo`, {
    method: 'POST',
    body: formData,
  });
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.detail || data.error || `HTTP ${response.status}`);
  }
  return data;
}

export function downloadJsonFile(filename, payload) {
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

export async function fetchScopeState(scope, department, unitId) {
  const url = new URL(`${API_V1}/alertas/estado-actual`, window.location.origin);
  url.searchParams.set('scope', scope);
  if (department) url.searchParams.set('department', department);
  if (unitId) url.searchParams.set('unit_id', unitId);
  return fetchJson(url.toString().replace(window.location.origin, ''));
}

export async function fetchWeatherForecast(scope, department, unitId) {
  const url = new URL(`${API_V1}/alertas/pronostico`, window.location.origin);
  url.searchParams.set('scope', scope);
  if (department) url.searchParams.set('department', department);
  if (unitId) url.searchParams.set('unit_id', unitId);
  return fetchJson(url.toString().replace(window.location.origin, ''));
}

export async function fetchHistory(scope, department, unitId, limit = 30) {
  const url = new URL(`${API_V1}/alertas/historico`, window.location.origin);
  url.searchParams.set('scope', scope);
  url.searchParams.set('limit', String(limit));
  if (department) url.searchParams.set('department', department);
  if (unitId) url.searchParams.set('unit_id', unitId);
  return fetchJson(url.toString().replace(window.location.origin, ''));
}

export async function fetchCustomState(geojson) {
  return fetchJson(`${API_V1}/alertas/unidad/custom`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(geojson),
  });
}

export async function fetchRiveraGeojson() {
  return fetchJson(`${API_BASE}/geojson/rivera`);
}

export async function fetchSettingsSchema() {
  return fetchJson(`${API_V1}/settings/schema`);
}

export async function fetchSettings(coverageClass = null) {
  const url = new URL(`${API_V1}/settings`, window.location.origin);
  if (coverageClass) url.searchParams.set('coverage_class', coverageClass);
  return fetchJson(url.toString().replace(window.location.origin, ''));
}

export async function fetchSettingsAudit(limit = 20) {
  const url = new URL(`${API_V1}/settings/audit`, window.location.origin);
  url.searchParams.set('limit', String(limit));
  return fetchJson(url.toString().replace(window.location.origin, ''));
}

export async function saveGlobalSettings(rules, operatorLabel = '') {
  return fetchJson(`${API_V1}/settings/global`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      rules,
      operator_label: operatorLabel || null,
      updated_from: 'settings_ui',
    }),
  });
}

export async function saveCoverageSettings(coverageClass, rules, operatorLabel = '') {
  return fetchJson(`${API_V1}/settings/overrides/${encodeURIComponent(coverageClass)}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      rules,
      operator_label: operatorLabel || null,
      updated_from: 'settings_ui',
    }),
  });
}

export async function resetGlobalSettings(operatorLabel = '') {
  return fetchJson(`${API_V1}/settings/reset/global`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      operator_label: operatorLabel || null,
      updated_from: 'settings_ui',
    }),
  });
}

export async function clearCoverageOverride(coverageClass, operatorLabel = '') {
  return fetchJson(`${API_V1}/settings/reset/${encodeURIComponent(coverageClass)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      operator_label: operatorLabel || null,
      updated_from: 'settings_ui',
    }),
  });
}
