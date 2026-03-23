const params = new URLSearchParams(window.location.search);
const isHttpOrigin = window.location.protocol === 'http:' || window.location.protocol === 'https:';
const defaultApiBase = isHttpOrigin ? '/api' : 'http://localhost:8000/api';

export const API_BASE =
  params.get('api') ||
  defaultApiBase;

export const API_V1 = `${API_BASE}/v1`;

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.detail || data.error || `HTTP ${response.status}`);
  }
  return data;
}

export async function fetchUnits() {
  return fetchJson(`${API_V1}/unidades`);
}

export async function fetchScopeState(scope, department, unitId) {
  const url = new URL(`${API_V1}/alertas/estado-actual`, window.location.origin);
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
