import { store } from './state.js?v=20260420-6';
import { recordFetch } from './diagnostics.js?v=20260420-6';

const params = new URLSearchParams(window.location.search);
const isHttpOrigin = window.location.protocol === 'http:' || window.location.protocol === 'https:';
const defaultApiBase = isHttpOrigin ? '/api' : 'http://localhost:8000/api';

export const API_BASE =
  params.get('api') ||
  defaultApiBase;

export const API_V1 = `${API_BASE}/v1`;
const SAFE_METHODS = new Set(['GET', 'HEAD', 'OPTIONS']);

async function fetchJson(url, options = {}) {
  const {
    suppressUnauthorizedEvent = false,
    includeCsrf = true,
    headers: inputHeaders = {},
    ...fetchOptions
  } = options;
  const method = (fetchOptions.method || 'GET').toUpperCase();
  const headers = new Headers(inputHeaders);
  if (!headers.has('Accept')) headers.set('Accept', 'application/json');
  if (includeCsrf && !SAFE_METHODS.has(method) && store.authCsrfToken && !headers.has('X-CSRF-Token')) {
    headers.set('X-CSRF-Token', store.authCsrfToken);
  }

  const _startedAt = performance.now();
  let response;
  try {
    response = await fetch(url, {
      credentials: 'same-origin',
      ...fetchOptions,
      method,
      headers,
    });
  } catch (networkErr) {
    try {
      recordFetch({
        url, method, status: null, ok: false,
        durationMs: performance.now() - _startedAt,
        error: networkErr?.message || String(networkErr),
      });
    } catch (_) { /* noop */ }
    throw networkErr;
  }
  const raw = await response.text();
  let data;
  try {
    data = raw ? JSON.parse(raw) : {};
  } catch {
    data = { detail: raw || `HTTP ${response.status}` };
  }
  try {
    recordFetch({
      url, method,
      status: response.status,
      ok: response.ok,
      durationMs: performance.now() - _startedAt,
      preview: response.ok ? undefined : data,
    });
  } catch (_) { /* noop */ }
  if (!response.ok) {
    let detail = data.detail || data.error || `HTTP ${response.status}`;
    if (Array.isArray(detail)) {
      detail = detail
        .map((item) => {
          if (!item || typeof item !== 'object') return String(item);
          const field = Array.isArray(item.loc) ? item.loc.filter((value) => value !== 'body').join('.') : '';
          return field ? `${field}: ${item.msg || 'Valor invalido'}` : (item.msg || 'Valor invalido');
        })
        .join(' | ');
    } else if (detail && typeof detail === 'object') {
      detail = detail.msg || JSON.stringify(detail);
    }
    const error = new Error(detail || `HTTP ${response.status}`);
    error.status = response.status;
    error.payload = data;
    if (response.status === 401 && !suppressUnauthorizedEvent) {
      window.dispatchEvent(new CustomEvent('agroclimax:unauthorized', { detail: data }));
    }
    throw error;
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
  return fetchJson(`${API_V1}/productivas/import-archivo`, {
    method: 'POST',
    body: formData,
  });
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

export async function fetchMapOverlayCatalog() {
  return fetchJson(`${API_V1}/map-overlays/catalog`);
}

export async function fetchTimelineFrames({ layers = [], dateFrom = null, dateTo = null, bbox = null, zoom = null } = {}) {
  const url = new URL(`${API_V1}/timeline/frames`, window.location.origin);
  layers.forEach((layerId) => url.searchParams.append('layers', layerId));
  if (dateFrom) url.searchParams.set('date_from', dateFrom);
  if (dateTo) url.searchParams.set('date_to', dateTo);
  if (bbox) url.searchParams.set('bbox', bbox);
  if (Number.isFinite(Number(zoom))) url.searchParams.set('zoom', String(zoom));
  return fetchJson(url.toString().replace(window.location.origin, ''));
}

export async function fetchTimelineContext({ scope, department = null, unitId = null, targetDate, historyDays = 30 } = {}) {
  const url = new URL(`${API_V1}/timeline/context`, window.location.origin);
  url.searchParams.set('scope', scope);
  url.searchParams.set('target_date', targetDate);
  url.searchParams.set('history_days', String(historyDays));
  if (department) url.searchParams.set('department', department);
  if (unitId) url.searchParams.set('unit_id', unitId);
  return fetchJson(url.toString().replace(window.location.origin, ''));
}

export async function startStartupPreload(payload = {}) {
  return fetchJson(`${API_V1}/preload/startup`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
}

export async function startViewportPreload(payload = {}) {
  return fetchJson(`${API_V1}/preload/viewport`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
}

export async function startTimelineWindowPreload(payload = {}) {
  return fetchJson(`${API_V1}/preload/timeline-window`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
}

export async function fetchPreloadStatus(runKey) {
  const url = new URL(`${API_V1}/preload/status`, window.location.origin);
  url.searchParams.set('run_key', runKey);
  return fetchJson(url.toString().replace(window.location.origin, ''));
}

export async function fetchNotificationEvents({ unitId = null, department = null, limit = 50 } = {}) {
  const url = new URL(`${API_V1}/notificaciones/eventos`, window.location.origin);
  if (unitId) url.searchParams.set('unit_id', unitId);
  if (department) url.searchParams.set('department', department);
  if (limit) url.searchParams.set('limit', String(limit));
  return fetchJson(url.toString().replace(window.location.origin, ''));
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

export async function fetchAuthMe() {
  return fetchJson(`${API_V1}/auth/me`, {
    suppressUnauthorizedEvent: true,
  });
}

export async function fetchProfileSchema() {
  return fetchJson(`${API_V1}/profile/schema`);
}

export async function fetchProfileMe() {
  return fetchJson(`${API_V1}/profile/me`);
}

export async function saveProfileMe(payload) {
  return fetchJson(`${API_V1}/profile/me`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
}

export async function fetchAlertSubscriptionOptions() {
  return fetchJson(`${API_V1}/alert-subscriptions/options`);
}

export async function fetchAlertSubscriptions() {
  return fetchJson(`${API_V1}/alert-subscriptions`);
}

export async function saveAlertSubscription(payload) {
  const method = payload?.id ? 'PUT' : 'POST';
  const url = payload?.id
    ? `${API_V1}/alert-subscriptions/${encodeURIComponent(payload.id)}`
    : `${API_V1}/alert-subscriptions`;
  return fetchJson(url, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
}

export async function deleteAlertSubscription(subscriptionId) {
  return fetchJson(`${API_V1}/alert-subscriptions/${encodeURIComponent(subscriptionId)}`, {
    method: 'DELETE',
  });
}

export async function testAlertSubscription(subscriptionId) {
  return fetchJson(`${API_V1}/alert-subscriptions/${encodeURIComponent(subscriptionId)}/test-send`, {
    method: 'POST',
  });
}

export async function searchPadron(department, padron) {
  const url = new URL(`${API_V1}/padrones/search`, window.location.origin);
  url.searchParams.set('department', department);
  url.searchParams.set('padron', padron);
  return fetchJson(url.toString().replace(window.location.origin, ''));
}

export async function fetchFarmOptions() {
  return fetchJson(`${API_V1}/campos/options`);
}

export async function fetchEstablishments() {
  return fetchJson(`${API_V1}/establecimientos`);
}

export async function saveEstablishment(payload, establishmentId = null) {
  const method = establishmentId ? 'PUT' : 'POST';
  const url = establishmentId
    ? `${API_V1}/establecimientos/${encodeURIComponent(establishmentId)}`
    : `${API_V1}/establecimientos`;
  return fetchJson(url, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
}

export async function deleteEstablishment(establishmentId) {
  return fetchJson(`${API_V1}/establecimientos/${encodeURIComponent(establishmentId)}`, {
    method: 'DELETE',
  });
}

export async function fetchFields(establishmentId = null) {
  const url = new URL(`${API_V1}/campos`, window.location.origin);
  if (establishmentId) url.searchParams.set('establishment_id', establishmentId);
  return fetchJson(url.toString().replace(window.location.origin, ''));
}

export async function fetchField(fieldId) {
  return fetchJson(`${API_V1}/campos/${encodeURIComponent(fieldId)}`);
}

export async function saveField(payload, fieldId = null) {
  const method = fieldId ? 'PUT' : 'POST';
  const url = fieldId
    ? `${API_V1}/campos/${encodeURIComponent(fieldId)}`
    : `${API_V1}/campos`;
  return fetchJson(url, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
}

export async function deleteField(fieldId) {
  return fetchJson(`${API_V1}/campos/${encodeURIComponent(fieldId)}`, {
    method: 'DELETE',
  });
}

export async function fetchFieldsGeojson(establishmentId = null) {
  const url = new URL(`${API_V1}/campos/geojson`, window.location.origin);
  if (establishmentId) url.searchParams.set('establishment_id', establishmentId);
  return fetchJson(url.toString().replace(window.location.origin, ''));
}

export async function fetchPaddocks(fieldId) {
  return fetchJson(`${API_V1}/campos/${encodeURIComponent(fieldId)}/potreros`);
}

export async function savePaddock(fieldId, payload, paddockId = null) {
  const method = paddockId ? 'PUT' : 'POST';
  const url = paddockId
    ? `${API_V1}/campos/${encodeURIComponent(fieldId)}/potreros/${encodeURIComponent(paddockId)}`
    : `${API_V1}/campos/${encodeURIComponent(fieldId)}/potreros`;
  return fetchJson(url, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
}

export async function deletePaddock(fieldId, paddockId) {
  return fetchJson(`${API_V1}/campos/${encodeURIComponent(fieldId)}/potreros/${encodeURIComponent(paddockId)}`, {
    method: 'DELETE',
  });
}

export async function fetchPaddocksGeojson(fieldId) {
  return fetchJson(`${API_V1}/campos/${encodeURIComponent(fieldId)}/potreros/geojson`);
}

export async function logoutCurrentUser() {
  return fetchJson(`${API_V1}/auth/logout`, {
    method: 'POST',
  });
}

export function googleLoginUrl(nextPath = null) {
  const nextValue = nextPath || `${window.location.pathname}${window.location.search}${window.location.hash}`;
  return `${API_V1}/auth/google/login?next=${encodeURIComponent(nextValue)}`;
}

export function profilePageUrl() {
  return '/perfil';
}
