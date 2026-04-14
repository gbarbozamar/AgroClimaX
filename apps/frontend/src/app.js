import { API_BASE, API_V1, downloadJsonFile, fetchCustomState, fetchDepartmentLayers, fetchHexagonsGeojson, fetchHistory, fetchMapOverlayCatalog, fetchPreloadStatus, fetchProductiveTemplate, fetchProductiveUnits, fetchProductiveUnitsGeojson, fetchScopeState, fetchSectionsGeojson, fetchTimelineContext, fetchUnits, fetchWeatherForecast, startStartupPreload, uploadProductiveUnitsFile } from './api.js';
import { initAuth } from './auth.js';
import { clearDepartmentLayer, clearHexLayer, clearProductiveLayer, clearSectionsLayer, highlightDepartment, highlightHex, highlightProductive, highlightSection, initMap, isLayerActive, refreshFarmPrivateOverlays, requestTimelineManifestRefresh, setAvailableOverlays, setHexesOnMap, setDepartmentsOnMap, setMapLayerChangeHandler, setProductivesOnMap, setSectionsOnMap, updateFocus } from './map.js';
import { initFieldsPanel } from './fields.js';
import { initEstablishmentViewerPanel } from './establishment-viewer.js';
import { initProfilePanel, refreshProfilePanel } from './profile.js';
import { normalizeState, populateDepartmentSelect, renderChart, renderDashboard, renderDrivers, renderError, renderForecast, renderHistory, renderLoading, renderWeatherCards } from './render.js';
import { initSettingsPanel } from './settings.js';
import { setStore, store } from './state.js';

setStore({ apiBase: API_BASE, apiV1: API_V1 });
const TIMELINE_CONTEXT_CACHE = new Map();
let timelineContextRequestSeq = 0;
let dashboardRequestSeq = 0;
const FRONTEND_PRELOAD_STAGES = {
  auth: { label: 'Sesion', detail: 'Validando acceso y tokens de la sesion.' },
  map: { label: 'Mapa base', detail: 'Inicializando viewport y controles.' },
  catalog: { label: 'Catalogos', detail: 'Cargando overlays, capas y opciones base.' },
  selection: { label: 'Contexto inicial', detail: 'Cargando estado, capas y seleccion inicial.' },
};
const BACKEND_PRELOAD_STAGE_META = {
  timeline_manifest: { label: 'Timeline historica', detail: 'Preparando el manifiesto temporal de la vista actual.' },
  timeline_context: { label: 'Metricas historicas', detail: 'Leyendo contexto historico materializado para la fecha actual y vecinas.' },
  analytic_neighbors: { label: 'Rasteres temporales', detail: 'Calentando tiles actuales y fechas vecinas para reducir buffering.' },
  official_overlays: { label: 'Overlays oficiales', detail: 'Cacheando overlays de viewport para reutilizar imagenes exportadas.' },
};
const HEADER_COLLAPSE_STORAGE_KEY = 'agroclimax.headerCollapsed';
const frontendPreloadState = {
  auth: { status: 'pending', detail: FRONTEND_PRELOAD_STAGES.auth.detail },
  map: { status: 'pending', detail: FRONTEND_PRELOAD_STAGES.map.detail },
  catalog: { status: 'pending', detail: FRONTEND_PRELOAD_STAGES.catalog.detail },
  selection: { status: 'pending', detail: FRONTEND_PRELOAD_STAGES.selection.detail },
};
const PRELOAD_MONITOR_WINDOW_MS = 45000;
const PRELOAD_MONITOR_RETRY_DELAY_MS = 1500;
const PRELOAD_MONITOR_ERROR_RETRY_DELAY_MS = 5000;
const PRELOAD_TERMINAL_STATUSES = new Set(['success', 'failed', 'missing', 'stale', 'superseded', 'completed']);
let preloadMonitorToken = 0;
let preloadMonitorRetryTimer = null;
let preloadMonitorAbortController = null;

function readHeaderCollapsedPreference() {
  try {
    return window.localStorage.getItem(HEADER_COLLAPSE_STORAGE_KEY) === '1';
  } catch (_) {
    return false;
  }
}

function writeHeaderCollapsedPreference(collapsed) {
  try {
    window.localStorage.setItem(HEADER_COLLAPSE_STORAGE_KEY, collapsed ? '1' : '0');
  } catch (_) {
    // ignore storage failures
  }
}

function renderHeaderCollapseToggle() {
  const collapsed = document.body.classList.contains('header-collapsed');
  const buttons = document.querySelectorAll('[data-header-collapse-toggle]');
  buttons.forEach((button) => {
    button.textContent = collapsed ? 'Mostrar header' : 'Plegar header';
    button.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
    button.title = collapsed ? 'Mostrar header' : 'Plegar header';
  });
}

function setHeaderCollapsed(collapsed) {
  document.body.classList.toggle('header-collapsed', Boolean(collapsed));
  writeHeaderCollapsedPreference(Boolean(collapsed));
  renderHeaderCollapseToggle();
}

function initHeaderCollapseToggle() {
  setHeaderCollapsed(readHeaderCollapsedPreference());
  const buttons = document.querySelectorAll('[data-header-collapse-toggle]');
  buttons.forEach((button) => {
    if (button.dataset.bound) return;
    button.dataset.bound = 'true';
    button.addEventListener('click', () => {
      setHeaderCollapsed(!document.body.classList.contains('header-collapsed'));
    });
  });
}

function historyContextFromV1(history) {
  return (history?.datos || []).map((item) => ({
    fecha: item.fecha,
    state: item.state,
    state_level: item.state_level,
    risk_score: item.risk_score,
    affected_pct: item.affected_pct,
  }));
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function addDays(isoDate, days) {
  const parsed = new Date(`${isoDate}T00:00:00Z`);
  if (Number.isNaN(parsed.getTime())) return isoDate;
  parsed.setUTCDate(parsed.getUTCDate() + days);
  return parsed.toISOString().slice(0, 10);
}

function sleep(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function preloadNodes() {
  return {
    overlay: document.getElementById('app-preload-overlay'),
    title: document.getElementById('app-preload-title'),
    copy: document.getElementById('app-preload-copy'),
    stageLabel: document.getElementById('app-preload-stage-label'),
    progressLabel: document.getElementById('app-preload-progress-label'),
    progressFill: document.getElementById('app-preload-progressfill'),
    stageList: document.getElementById('app-preload-stage-list'),
    footnote: document.getElementById('app-preload-footnote'),
    mini: document.getElementById('app-preload-mini'),
    miniTitle: document.getElementById('app-preload-mini-title'),
    miniDetail: document.getElementById('app-preload-mini-detail'),
    miniProgress: document.getElementById('app-preload-mini-progress'),
  };
}

function setFrontendPreloadStage(stageKey, status, detail = null) {
  if (!frontendPreloadState[stageKey]) return;
  frontendPreloadState[stageKey] = {
    ...frontendPreloadState[stageKey],
    status,
    detail: detail || FRONTEND_PRELOAD_STAGES[stageKey].detail,
  };
  renderPreloadUi();
}

function preloadResidualOnlyState() {
  const status = store.preloadStatus || {};
  const residualStage = status.residual_stage || status.details?.residual_stage || null;
  return Boolean(
    store.preloadCriticalReady
    && residualStage === 'official_overlays'
    && !PRELOAD_TERMINAL_STATUSES.has(String(status.status || '').toLowerCase()),
  );
}

function combinedPreloadStages() {
  const frontendStages = Object.entries(FRONTEND_PRELOAD_STAGES).map(([key, meta]) => ({
    key,
    label: meta.label,
    detail: frontendPreloadState[key]?.detail || meta.detail,
    status: frontendPreloadState[key]?.status || 'pending',
    done: frontendPreloadState[key]?.status === 'done' ? 1 : 0,
    total: 1,
  }));
  const backendDetails = store.preloadStatus?.details || {};
  const backendStages = Object.entries(backendDetails.stages || {}).map(([key, value]) => ({
    key,
    label: BACKEND_PRELOAD_STAGE_META[key]?.label || key,
    detail: BACKEND_PRELOAD_STAGE_META[key]?.detail || '',
    status: value?.status || 'pending',
    done: Number(value?.done || 0),
    total: Math.max(1, Number(value?.total || 0)),
  }));
  return [...frontendStages, ...backendStages];
}

function preloadProgressRatio({ excludeResidualOfficialOverlays = false } = {}) {
  let stages = combinedPreloadStages();
  if (excludeResidualOfficialOverlays) {
    stages = stages.filter((stage) => stage.key !== 'official_overlays');
  }
  if (!stages.length) return 0;
  const total = stages.reduce((acc, stage) => acc + Math.max(0, Number(stage.total || 0)), 0);
  if (total <= 0) return 1;
  const done = stages.reduce((acc, stage) => {
    if (stage.status === 'done') return acc + Math.max(0, Number(stage.total || 0));
    return acc + Math.min(Number(stage.done || 0), Math.max(0, Number(stage.total || 0)));
  }, 0);
  return Math.max(0, Math.min(1, done / total));
}

function renderPreloadUi() {
  const nodes = preloadNodes();
  const stages = combinedPreloadStages();
  const residualOnly = preloadResidualOnlyState();
  const progress = preloadProgressRatio({ excludeResidualOfficialOverlays: residualOnly });
  const currentStage = stages.find((stage) => stage.status === 'running') || stages.find((stage) => stage.status === 'pending') || stages[stages.length - 1];
  const progressPct = Math.round(progress * 100);
  const backgroundWarming = Boolean(
    store.preloadCriticalReady
    && store.preloadStatus
    && !['success', 'failed'].includes(store.preloadStatus.status),
  );
  const overlayVisible = Boolean(store.preloadVisible && !store.preloadCriticalReady);
  const miniVisible = Boolean(store.preloadMiniVisible || backgroundWarming);
  if (nodes.overlay) nodes.overlay.classList.toggle('hidden', !overlayVisible);
  if (nodes.title) nodes.title.textContent = residualOnly
    ? 'App lista, completando overlays oficiales'
    : (store.preloadCriticalReady ? 'App lista, completando cache residual' : 'Preparando capas y cache inicial');
  if (nodes.copy) nodes.copy.textContent = residualOnly
    ? 'La parte critica ya esta lista. Los overlays oficiales siguen calentando en segundo plano y ya no bloquean la app.'
    : (store.preloadCriticalReady
      ? 'La parte critica ya esta lista. El resto sigue calentando cache en segundo plano.'
      : 'Cargando sesion, catalogos y frames temporales para reducir buffering inicial.');
  if (nodes.stageLabel) nodes.stageLabel.textContent = residualOnly ? 'Listo para usar' : (currentStage ? currentStage.label : 'Iniciando...');
  if (nodes.progressLabel) nodes.progressLabel.textContent = residualOnly ? 'Listo' : `${progressPct}%`;
  if (nodes.progressFill) nodes.progressFill.style.width = residualOnly ? '100%' : `${progressPct}%`;
  if (nodes.stageList) {
    nodes.stageList.innerHTML = stages.map((stage) => {
      const className = stage.status === 'done' ? ' is-done' : (stage.status === 'running' ? ' is-running' : '');
      const summary = stage.total > 1
        ? `${stage.done}/${stage.total}`
        : stage.total === 1
          ? (stage.status === 'done' ? 'Listo' : stage.status === 'running' ? 'En curso' : 'Pendiente')
          : 'N/A';
      return `
        <div class="app-preload-stage${className}">
          <div>
            <div class="app-preload-stage-name">${stage.label}</div>
            <div class="app-preload-stage-detail">${stage.detail || ''}</div>
          </div>
          <div class="app-preload-stage-status">${summary}</div>
        </div>
      `;
    }).join('');
  }
  if (nodes.footnote) {
    nodes.footnote.textContent = residualOnly
      ? 'El sistema principal ya esta listo. Los overlays oficiales restantes son trabajo residual no critico.'
      : (store.preloadCriticalReady
        ? 'Se liberaron las partes criticas. El calentamiento residual sigue para mejorar reproduccion y cambios de capa.'
        : 'La app se habilita apenas lo critico queda listo. El resto sigue calentando en segundo plano.');
  }
  if (nodes.mini) nodes.mini.classList.toggle('hidden', !store.preloadMiniVisible);
  if (nodes.miniTitle) nodes.miniTitle.textContent = residualOnly
    ? 'Listo para usar'
    : (store.preloadCriticalReady ? 'Cache residual en progreso' : 'Precarga inicial');
  if (nodes.miniDetail) {
    const stageKey = store.preloadStatus?.stage;
    nodes.miniDetail.textContent = residualOnly
      ? 'Completando overlays oficiales en segundo plano.'
      : (BACKEND_PRELOAD_STAGE_META[stageKey]?.label || currentStage?.label || 'Preparando cache');
  }
  if (nodes.miniProgress) nodes.miniProgress.textContent = residualOnly ? 'Listo' : `${progressPct}%`;
  if (nodes.mini) nodes.mini.classList.toggle('hidden', !miniVisible);
  document.body.classList.toggle('preload-blocked', overlayVisible);
  setStore({ preloadProgress: progress });
}

function selectionPreloadPayload(descriptor = currentSelectionDescriptor()) {
  if (!descriptor || descriptor.scope === 'custom') {
    return {
      scope_type: 'viewport',
      scope_ref: 'custom',
      timeline_scope: 'nacional',
      timeline_unit_id: null,
      timeline_department: null,
    };
  }
  if (descriptor.scope === 'unidad') {
    const selectionKind = descriptor.selectionKind || descriptor.unitMeta?.selection_kind || 'unidad';
    const scopeRef = descriptor.unitId
      || descriptor.unitMeta?.unit_id
      || descriptor.unitMeta?.source_paddock_id
      || descriptor.unitMeta?.source_field_id
      || null;
    return {
      scope_type: selectionKind,
      scope_ref: scopeRef,
      timeline_scope: 'unidad',
      timeline_unit_id: descriptor.unitId || null,
      timeline_department: descriptor.unitMeta?.department || null,
    };
  }
  if (descriptor.scope === 'departamento') {
    return {
      scope_type: 'departamento',
      scope_ref: descriptor.department || 'departamento',
      timeline_scope: 'departamento',
      timeline_unit_id: null,
      timeline_department: descriptor.department || null,
    };
  }
  return {
    scope_type: 'nacional',
    scope_ref: 'Uruguay',
    timeline_scope: 'nacional',
    timeline_unit_id: null,
    timeline_department: null,
  };
}

function currentViewportPreloadPayload() {
  const mapNode = document.getElementById('map');
  const bounds = store.map?.getBounds?.();
  const bbox = bounds
    ? [bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth()].map((value) => Number(value).toFixed(4)).join(',')
    : null;
  return {
    bbox,
    zoom: store.map ? Math.round(store.map.getZoom()) : 7,
    width: Math.max(256, Math.round(mapNode?.clientWidth || 1024)),
    height: Math.max(256, Math.round(mapNode?.clientHeight || 640)),
  };
}

async function pollPreloadRun(runKey, { timeoutMs = 0, stopOnCritical = false, shouldContinue = null, signal = null } = {}) {
  const startedAt = Date.now();
  let latest = null;
  while (true) {
    if (signal?.aborted) return null;
    if (shouldContinue && !shouldContinue()) return null;
    try {
      latest = await fetchPreloadStatus(runKey, { signal });
    } catch (error) {
      if (error?.name === 'AbortError' || signal?.aborted) return null;
      throw error;
    }
    if (signal?.aborted) return null;
    if (shouldContinue && !shouldContinue()) return null;
    if (store.preloadRunKey && store.preloadRunKey !== runKey) return null;
    setStore({
      preloadStatus: latest,
      preloadCriticalReady: Boolean(latest.critical_ready),
    });
    renderPreloadUi();
    if (PRELOAD_TERMINAL_STATUSES.has(String(latest.status || '').toLowerCase())) return latest;
    if (stopOnCritical && latest.critical_ready) return latest;
    if (timeoutMs > 0 && (Date.now() - startedAt) >= timeoutMs) return latest;
    if (shouldContinue && !shouldContinue()) return null;
    await sleep(500);
  }
}

function stopPreloadMonitoring({ hideMini = false } = {}) {
  preloadMonitorToken += 1;
  if (preloadMonitorRetryTimer) {
    window.clearTimeout(preloadMonitorRetryTimer);
    preloadMonitorRetryTimer = null;
  }
  if (preloadMonitorAbortController) {
    preloadMonitorAbortController.abort();
    preloadMonitorAbortController = null;
  }
  if (hideMini) {
    setStore({ preloadMiniVisible: false });
    renderPreloadUi();
  }
}

function continuePreloadMonitoring(runKey) {
  if (!runKey) return;
  stopPreloadMonitoring();
  const token = preloadMonitorToken;
  const abortController = new AbortController();
  preloadMonitorAbortController = abortController;

  const scheduleRetry = (delayMs) => {
    if (token !== preloadMonitorToken) return;
    preloadMonitorRetryTimer = window.setTimeout(() => {
      preloadMonitorRetryTimer = null;
      void monitor();
    }, delayMs);
  };

  const monitor = async () => {
    if (token !== preloadMonitorToken) return;
    try {
      const latest = await pollPreloadRun(runKey, {
        timeoutMs: PRELOAD_MONITOR_WINDOW_MS,
        stopOnCritical: false,
        shouldContinue: () => token === preloadMonitorToken && store.preloadRunKey === runKey,
        signal: abortController.signal,
      });
      if (token !== preloadMonitorToken) return;
      if (!latest) return;
      if (PRELOAD_TERMINAL_STATUSES.has(String(latest?.status || '').toLowerCase())) {
        stopPreloadMonitoring({ hideMini: true });
        return;
      }
      scheduleRetry(PRELOAD_MONITOR_RETRY_DELAY_MS);
    } catch (error) {
      if (token !== preloadMonitorToken) return;
      console.warn('No se pudo completar el monitoreo de precarga:', error);
      scheduleRetry(PRELOAD_MONITOR_ERROR_RETRY_DELAY_MS);
    }
  };

  void monitor();
}

function releasePreloadOverlay() {
  const stillRunning = Boolean(store.preloadRunKey && store.preloadStatus && !PRELOAD_TERMINAL_STATUSES.has(String(store.preloadStatus.status || '').toLowerCase()));
  setStore({
    preloadVisible: false,
    preloadMiniVisible: stillRunning,
  });
  renderPreloadUi();
}

function isHistoricalTimelineDate(targetDate = store.timelineDate) {
  return Boolean(store.timelineEnabled && targetDate && targetDate !== todayIsoDate());
}

function setTimelineForecastVisibility(collapsed) {
  const weatherPanel = document.getElementById('weather-strip-panel');
  const forecastPanel = document.getElementById('forecast-panel-section');
  if (weatherPanel) weatherPanel.style.display = collapsed ? 'none' : '';
  if (forecastPanel) forecastPanel.style.display = collapsed ? 'none' : '';
  setStore({ timelineForecastCollapsed: collapsed });
}

function selectedFieldUnitMeta() {
  const field = store.selectedFieldDetail
    || (store.farmFields || []).find((item) => item.id === store.selectedFieldId)
    || null;
  if (!field) return null;
  return {
    unit_id: field.aoi_unit_id || null,
    unit_name: field.name || 'Campo',
    name: field.name || 'Campo',
    centroid_lat: field.centroid_lat ?? null,
    centroid_lon: field.centroid_lon ?? null,
    department: field.department || null,
    selection_kind: 'field',
    source_field_id: field.id,
  };
}

function selectedPaddockUnitMeta() {
  const field = store.selectedFieldDetail
    || (store.farmFields || []).find((item) => item.id === store.selectedFieldId)
    || null;
  const paddock = (field?.paddocks || []).find((item) => item.id === store.selectedPaddockId) || null;
  if (!paddock) return null;
  return {
    unit_id: paddock.aoi_unit_id || null,
    unit_name: paddock.name || 'Potrero',
    name: paddock.name || 'Potrero',
    centroid_lat: field?.centroid_lat ?? null,
    centroid_lon: field?.centroid_lon ?? null,
    department: field?.department || null,
    selection_kind: 'paddock',
    source_field_id: field?.id || null,
    source_paddock_id: paddock.id,
  };
}

function currentFarmSelectionDescriptor() {
  const paddockMeta = selectedPaddockUnitMeta();
  if (paddockMeta) {
    return {
      scope: 'unidad',
      unitId: paddockMeta.unit_id,
      unitMeta: paddockMeta,
      supported: Boolean(paddockMeta.unit_id),
      selectionKind: 'paddock',
    };
  }

  const fieldMeta = selectedFieldUnitMeta();
  if (fieldMeta) {
    return {
      scope: 'unidad',
      unitId: fieldMeta.unit_id,
      unitMeta: fieldMeta,
      supported: Boolean(fieldMeta.unit_id),
      selectionKind: 'field',
    };
  }

  return null;
}

function currentSelectionDescriptor() {
  if (store.customGeojson) return { scope: 'custom', supported: false };
  const farmSelection = currentFarmSelectionDescriptor();
  if (farmSelection) return farmSelection;
  if (store.selectedProductiveId) {
    return {
      scope: 'unidad',
      unitId: store.selectedProductiveId,
      unitMeta: selectedProductiveProps(store.selectedProductiveId),
      supported: true,
    };
  }
  if (store.selectedSectionId) {
    return {
      scope: 'unidad',
      unitId: store.selectedSectionId,
      unitMeta: selectedSectionProps(store.selectedSectionId),
      supported: true,
    };
  }
  if (store.selectedHexId) {
    return {
      scope: 'unidad',
      unitId: store.selectedHexId,
      unitMeta: selectedHexProps(store.selectedHexId),
      supported: true,
    };
  }
  if (store.selectedScope === 'departamento' && store.selectedDepartment) {
    return {
      scope: 'departamento',
      department: store.selectedDepartment,
      unitMeta: store.units.find((item) => item.department === store.selectedDepartment) || null,
      supported: true,
    };
  }
  if (store.selectedScope === 'unidad' && store.selectedUnitId) {
    const unitMeta = selectedProductiveProps(store.selectedUnitId)
      || selectedSectionProps(store.selectedUnitId)
      || selectedHexProps(store.selectedUnitId)
      || store.units.find((item) => item.id === store.selectedUnitId)
      || null;
    return {
      scope: 'unidad',
      unitId: store.selectedUnitId,
      unitMeta,
      supported: true,
    };
  }
  return { scope: 'nacional', department: null, unitId: null, unitMeta: null, supported: true };
}

function timelineContextCacheKey({ scope, department = null, unitId = null, targetDate, historyDays = 30 }) {
  return `${scope}|${department || '-'}|${unitId || '-'}|${targetDate}|${historyDays}`;
}

async function fetchTimelineContextCached({ scope, department = null, unitId = null, targetDate, historyDays = 30 }) {
  const cacheKey = timelineContextCacheKey({ scope, department, unitId, targetDate, historyDays });
  if (TIMELINE_CONTEXT_CACHE.has(cacheKey)) return TIMELINE_CONTEXT_CACHE.get(cacheKey);
  const payload = await fetchTimelineContext({ scope, department, unitId, targetDate, historyDays });
  TIMELINE_CONTEXT_CACHE.set(cacheKey, payload);
  return payload;
}

function buildTimelineModel(contextPayload, descriptor) {
  const statePayload = { ...(contextPayload?.state_payload || {}) };
  const historyPayload = contextPayload?.history_payload || { datos: [] };
  const scopeLabel = contextPayload?.selection_label
    || statePayload.department
    || descriptor?.unitMeta?.unit_name
    || descriptor?.unitMeta?.name
    || 'Uruguay';
  const model = normalizeState(statePayload, {
    history: historyContextFromV1(historyPayload),
    unitLat: descriptor?.unitMeta?.centroid_lat ?? null,
    unitLon: descriptor?.unitMeta?.centroid_lon ?? null,
    scopeLabel,
  });
  if (contextPayload?.forecast_mode !== 'current_live') {
    model.forecast = [];
    model.technical = { ...(model.technical || {}), forecast: 'No aplica en timeline historica' };
  }
  return model;
}

function applyDashboardModel(model, { renderForecastPanel = true, renderWeather = true } = {}) {
  renderDashboard(model);
  renderDrivers(model);
  renderHistory(model);
  setStore({
    chart: renderChart(model, store.chart),
    currentModel: model,
  });
  if (renderForecastPanel) renderForecast(model);
  if (renderWeather) {
    syncWeatherFilterOptions();
    return refreshWeatherCards();
  }
  syncWeatherFilterOptions();
  return Promise.resolve();
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
  if (store.selectedPaddockId) {
    const meta = selectedPaddockUnitMeta();
    if (meta?.unit_id) {
      return {
        value: 'current',
        label: `Actual: ${meta.unit_name || 'potrero'}`,
        mode: 'current',
        scope: 'unidad',
        unitId: meta.unit_id,
      };
    }
  }
  if (store.selectedFieldId) {
    const meta = selectedFieldUnitMeta();
    if (meta?.unit_id) {
      return {
        value: 'current',
        label: `Actual: ${meta.unit_name || 'campo'}`,
        mode: 'current',
        scope: 'unidad',
        unitId: meta.unit_id,
      };
    }
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

  if (isLayerActive('judicial') && store.sectionsLookup) {
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

async function prefetchTimelineContextNeighbors(descriptor, targetDate) {
  if (!descriptor?.supported) return;
  const neighborDates = [addDays(targetDate, -1), addDays(targetDate, 1)];
  await Promise.all(
    neighborDates.map(async (dateValue) => {
      try {
        await fetchTimelineContextCached({
          scope: descriptor.scope,
          department: descriptor.department || null,
          unitId: descriptor.unitId || null,
          targetDate: dateValue,
          historyDays: 30,
        });
      } catch (error) {
        console.warn('No se pudo precargar contexto historico:', error);
      }
    }),
  );
}

async function refreshDashboardFromTimelineDate(targetDate = store.timelineDate, { silent = false, requestSeq: incomingRequestSeq = null } = {}) {
  const descriptor = currentSelectionDescriptor();
  if (!descriptor?.supported || !targetDate || !isHistoricalTimelineDate(targetDate)) {
    setTimelineForecastVisibility(false);
    return false;
  }

  const effectiveRequestSeq = Number.isFinite(incomingRequestSeq) ? incomingRequestSeq : ++dashboardRequestSeq;
  const timelineRequestSeq = ++timelineContextRequestSeq;
  setStore({ timelineContextLoading: true, timelineContextRequestSeq: timelineRequestSeq });
  if (!silent) renderLoading(`Cargando timeline ${targetDate}...`);

  try {
    const contextPayload = await fetchTimelineContextCached({
      scope: descriptor.scope,
      department: descriptor.department || null,
      unitId: descriptor.unitId || null,
      targetDate,
      historyDays: 30,
    });
    if (effectiveRequestSeq !== dashboardRequestSeq || timelineRequestSeq !== timelineContextRequestSeq) return true;
    if (!contextPayload?.state_payload) {
      setStore({ timelineContextLoading: false });
      if (!silent) renderError('No hay contexto historico materializado para la fecha seleccionada.');
      return false;
    }
    const model = buildTimelineModel(contextPayload, descriptor);
    setStore({ timelineContext: contextPayload, timelineContextLoading: false });
    setTimelineForecastVisibility(true);
    await applyDashboardModel(model, { renderForecastPanel: false, renderWeather: false });
    updateFocus(model);
    await prefetchTimelineContextNeighbors(descriptor, targetDate);
    return true;
  } catch (error) {
    if (effectiveRequestSeq !== dashboardRequestSeq || timelineRequestSeq !== timelineContextRequestSeq) return true;
    setStore({ timelineContextLoading: false });
    if (!silent) renderError(`No se pudo cargar el contexto historico: ${error.message}`);
    return false;
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
  const units = Array.isArray(payload?.datos) ? payload.datos : [];
  setStore({ units });
  populateDepartmentSelect(units, store.selectedDepartment || 'nacional');
}

function populateDepartmentSelectFromFeatureCollection(featureCollection, selectedDepartment = null) {
  const departments = Array.from(
    new Set(
      (featureCollection?.features || [])
        .map((feature) => feature?.properties?.department || feature?.properties?.name || null)
        .filter(Boolean),
    ),
  ).sort((a, b) => String(a).localeCompare(String(b), 'es'));
  if (!departments.length) return;
  populateDepartmentSelect(
    departments.map((department) => ({ department })),
    selectedDepartment || store.selectedDepartment || 'nacional',
  );
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
    refreshFarmPrivateOverlays();
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
    populateDepartmentSelectFromFeatureCollection(collection, selectedDepartment);
    setDepartmentsOnMap(collection, handleDepartmentSelect, selectedDepartment);
    if (selectedDepartment) highlightDepartment(selectedDepartment, true);
    refreshFarmPrivateOverlays();
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
    refreshFarmPrivateOverlays();
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
    refreshFarmPrivateOverlays();
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
  const requestSeq = ++dashboardRequestSeq;
  renderLoading(scope === 'nacional' ? 'Cargando panorama nacional...' : `Cargando ${department || 'unidad'}...`);
  try {
    let data;
    let history;
    let unit = null;
    setStore({
      selectedScope: scope,
      selectedDepartment: department,
      selectedUnitId: unitId,
    });
    if (scope === 'custom' && store.customGeojson) {
      data = await fetchCustomState(store.customGeojson);
      history = { datos: [] };
    } else {
      unit = store.units.find((item) => item.department === department || item.id === unitId) || selectedSectionProps(unitId) || selectedProductiveProps(unitId) || selectedHexProps(unitId) || null;
      if (isHistoricalTimelineDate()) {
        const rendered = await refreshDashboardFromTimelineDate(store.timelineDate, { silent: false, requestSeq });
        if (rendered) {
          if (unitId && isLayerActive('judicial')) highlightSection(unitId, false);
          if (unitId && isLayerActive('productiva')) highlightProductive(unitId, false);
          if (unitId && isLayerActive('hex')) highlightHex(unitId, false);
          if (department && !isLayerActive('judicial')) highlightDepartment(department, false);
          return;
        }
      }
      data = await fetchScopeState(scope, department, unitId);
      history = await fetchHistory(scope, department, unitId, 30);
    }
    if (requestSeq !== dashboardRequestSeq) return;

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
    setTimelineForecastVisibility(false);
    setStore({
      chart: renderChart(model, store.chart),
      currentModel: model,
      timelineContext: null,
    });
    syncWeatherFilterOptions();
    await refreshWeatherCards();
    updateFocus(model);
    if (unitId && isLayerActive('judicial')) highlightSection(unitId, false);
    if (unitId && isLayerActive('productiva')) highlightProductive(unitId, false);
    if (unitId && isLayerActive('hex')) highlightHex(unitId, false);
    if (department && !isLayerActive('judicial')) highlightDepartment(department, false);
  } catch (error) {
    renderError(`No se pudo cargar el dashboard: ${error.message}`);
  }
}

async function handleDepartmentSelect(department) {
  const select = document.getElementById('department-select');
  if (select) select.value = department;
  setStore({
    customGeojson: null,
    selectedSectionId: null,
    selectedHexId: null,
    selectedFieldId: null,
    selectedPaddockId: null,
    selectedFieldDetail: null,
    viewportUserPinned: false,
    viewportProgrammaticEvents: 0,
  });
  setStore({ selectedProductiveId: null, selectedScope: 'departamento', selectedDepartment: department, selectedUnitId: null });
  document.getElementById('btn-limpiar').style.display = 'none';
  await refreshProductiveImportSummary(department);
  await loadSelection('departamento', department, null);
  if (isLayerActive('judicial')) {
    await loadSectionsLayer(department);
    requestTimelineManifestRefresh({ preserveDate: false });
    return;
  }
  if (isLayerActive('productiva')) {
    await loadProductiveLayer(department);
    requestTimelineManifestRefresh({ preserveDate: false });
    return;
  }
  await loadDepartmentLayer(department);
  setStore({ selectedScope: 'departamento', selectedDepartment: department, selectedUnitId: null });
  requestTimelineManifestRefresh({ preserveDate: false });
}

async function handleSectionSelect(section) {
  setStore({ customGeojson: null, selectedSectionId: section.unit_id, selectedProductiveId: null, selectedHexId: null, viewportUserPinned: false, viewportProgrammaticEvents: 0 });
  await loadSelection('unidad', null, section.unit_id);
}

async function handleProductiveSelect(unit) {
  setStore({ customGeojson: null, selectedProductiveId: unit.unit_id, selectedSectionId: null, selectedHexId: null, viewportUserPinned: false, viewportProgrammaticEvents: 0 });
  await loadSelection('unidad', null, unit.unit_id);
}

async function handleHexSelect(hex) {
  setStore({ customGeojson: null, selectedHexId: hex.unit_id, selectedProductiveId: null, selectedSectionId: null, viewportUserPinned: false, viewportProgrammaticEvents: 0 });
  await loadSelection('unidad', null, hex.unit_id);
}

async function refreshCurrentSelection() {
  if (store.customGeojson) {
    await loadSelection('custom');
    return;
  }
  const farmSelection = currentFarmSelectionDescriptor();
  if (farmSelection?.supported && farmSelection.unitId) {
    await loadSelection('unidad', null, farmSelection.unitId);
    return;
  }
  if (farmSelection) {
    setTimelineForecastVisibility(false);
    syncWeatherFilterOptions();
    await refreshWeatherCards();
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
  if (isLayerActive('judicial')) await loadSectionsLayer(department);
  else clearSectionsLayer();

  if (isLayerActive('productiva')) await loadProductiveLayer(department);
  else clearProductiveLayer();

  if (isLayerActive('hex')) await loadHexLayer(department);
  else clearHexLayer();

  if (isLayerActive('judicial') || isLayerActive('productiva') || isLayerActive('hex')) {
    clearDepartmentLayer();
  } else {
    await loadDepartmentLayer(store.selectedDepartment || department || null);
  }

  refreshFarmPrivateOverlays();
  syncWeatherFilterOptions();
}

async function handleTimelineDateChange(event) {
  const targetDate = event?.detail?.date || store.timelineDate || todayIsoDate();
  const enabled = Boolean(event?.detail?.enabled);
  if (!enabled || !isHistoricalTimelineDate(targetDate)) {
    setTimelineForecastVisibility(false);
    await refreshCurrentSelection();
    return;
  }
  await refreshDashboardFromTimelineDate(targetDate, { silent: true });
}

async function bootstrap() {
  initHeaderCollapseToggle();
  setStore({
    preloadVisible: true,
    preloadMiniVisible: false,
    preloadRunKey: null,
    preloadStatus: null,
    preloadCriticalReady: false,
  });
  renderPreloadUi();
  setFrontendPreloadStage('auth', 'running');
  const authenticated = await initAuth();
  if (!authenticated) {
    setStore({ preloadVisible: false, preloadMiniVisible: false });
    renderPreloadUi();
    return;
  }
  setFrontendPreloadStage('auth', 'done', 'Sesion validada.');

  const timelineChangeBridge = (detail) => {
    handleTimelineDateChange({ detail }).catch((error) => {
      console.warn('No se pudo sincronizar la timeline con el dashboard:', error);
    });
  };
  setStore({ onTimelineDateChange: timelineChangeBridge });
  window.addEventListener('agroclimax:timeline-date-change', (event) => {
    if (event?.detail?._handledByStoreCallback) return;
    timelineChangeBridge(event?.detail || {});
  });
  window.addEventListener('agroclimax:viewport-preload-started', (event) => {
    const payload = event?.detail || {};
    if (!payload.run_key) return;
    stopPreloadMonitoring();
    setStore({
      preloadRunKey: payload.run_key,
      preloadStatus: payload,
      preloadMiniVisible: true,
    });
    renderPreloadUi();
    continuePreloadMonitoring(payload.run_key);
  });

  setFrontendPreloadStage('map', 'running');
  await initMap(async (geojson) => {
    setStore({ customGeojson: geojson, selectedSectionId: null, selectedProductiveId: null, selectedHexId: null });
    await loadSelection('custom');
  }, handleDepartmentSelect, handleSectionSelect);
  setFrontendPreloadStage('map', 'done', 'Viewport y controles inicializados.');
  setMapLayerChangeHandler(async () => {
    const preserveViewport = Boolean(store.selectedFieldId || store.selectedPaddockId);
    const preservedCenter = preserveViewport && store.map ? store.map.getCenter() : null;
    const preservedZoom = preserveViewport && store.map ? store.map.getZoom() : null;
    await refreshCurrentLayer();
    if (preserveViewport && preservedCenter && Number.isFinite(preservedZoom)) {
      store.map.setView(preservedCenter, preservedZoom, { animate: false });
      refreshFarmPrivateOverlays();
    }
  });

  setFrontendPreloadStage('catalog', 'running');
  try {
    const overlayCatalog = await fetchMapOverlayCatalog();
    setAvailableOverlays(overlayCatalog.items || []);
    setFrontendPreloadStage('catalog', 'done', `Catalogo listo con ${(overlayCatalog.items || []).length} overlays.`);
  } catch (error) {
    console.warn('No se pudo cargar el catalogo de overlays oficiales:', error);
    setAvailableOverlays([]);
    setFrontendPreloadStage('catalog', 'done', 'Catalogo de overlays no disponible; se sigue con fallback local.');
  }

  initSettingsPanel({
    onRefreshSelection: refreshCurrentSelection,
    onRefreshLayers: refreshCurrentLayer,
  });
  initEstablishmentViewerPanel();
  await initFieldsPanel();
  initProfilePanel();

  const select = document.getElementById('department-select');
  const weatherSelect = document.getElementById('weather-filter-select');
  select.addEventListener('change', async (event) => {
    const value = event.target.value;
    setStore({
      customGeojson: null,
      selectedSectionId: null,
      selectedProductiveId: null,
      selectedHexId: null,
      selectedFieldId: null,
      selectedPaddockId: null,
      selectedFieldDetail: null,
      viewportUserPinned: false,
      viewportProgrammaticEvents: 0,
    });
    if (value === 'nacional') {
      await refreshProductiveImportSummary(null);
      await loadSelection('nacional');
      requestTimelineManifestRefresh({ preserveDate: false });
      await refreshCurrentLayer();
      return;
    }
    await handleDepartmentSelect(value);
  });

  weatherSelect?.addEventListener('change', async (event) => {
    setStore({ weatherFilterValue: event.target.value });
    await refreshWeatherCards();
  });

  setFrontendPreloadStage('selection', 'running');
  await loadUnitsSafe();
  const initialDepartmentLayerPromise = loadDepartmentLayer(null);
  setStore({
    preloadCriticalReady: true,
    preloadMiniVisible: false,
  });
  releasePreloadOverlay();
  await loadSelection('nacional');
  setFrontendPreloadStage('selection', 'done', 'Contexto inicial y capas base listas.');
  await initialDepartmentLayerPromise;

  requestTimelineManifestRefresh({ preserveDate: false });

  const preloadSelection = selectionPreloadPayload();
  const preloadViewport = currentViewportPreloadPayload();
  let startupPreloadRunKey = null;
  startStartupPreload({
    ...preloadViewport,
    temporal_layers: (store.activeLayers || []).filter((layerId) => ['alerta', 'rgb', 'ndvi', 'ndmi', 'ndwi', 'savi', 'sar', 'lst'].includes(layerId)),
    official_layers: (store.availableOverlays || []).filter((item) => item.recommended).map((item) => item.id),
    ...preloadSelection,
    target_date: todayIsoDate(),
    history_days: 30,
  }).then((preloadResponse) => {
    startupPreloadRunKey = preloadResponse?.run_key || null;
    stopPreloadMonitoring();
    setStore({
      preloadRunKey: startupPreloadRunKey,
      preloadStatus: preloadResponse || null,
      preloadMiniVisible: Boolean(startupPreloadRunKey),
    });
    renderPreloadUi();
    if (startupPreloadRunKey) {
      continuePreloadMonitoring(startupPreloadRunKey);
    }
  }).catch((error) => {
    console.warn('No se pudo iniciar la precarga de startup:', error);
  });

  syncWeatherFilterOptions();
  await refreshWeatherCards();
  await refreshProductiveImportSummary(null);
  wireProductiveImportControls();
  await refreshProfilePanel();
  setProductiveImportStatus('Subi un .geojson o .zip shapefile para activar la capa Predios.', 'muted');
}

document.addEventListener('DOMContentLoaded', bootstrap);
