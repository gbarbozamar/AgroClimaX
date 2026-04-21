import { API_BASE, API_V1, fetchNotificationEvents, fetchTimelineFrames, startTimelineWindowPreload, startViewportPreload } from './api.js?v=20260420-6';
import { store, setStore } from './state.js?v=20260420-6';

const CONEAT_MIN_VISIBLE_ZOOM = 11;
const INITIAL_VIEW = { center: [-32.8, -56.0], zoom: 7 };
// Bbox Uruguay continental con ~10 km de padding en cada lado para que el pan
// no se sienta bloqueado en la frontera. Fuera de este rectángulo Leaflet no
// pide tiles — evita cargar pampa argentina, Brasil y océano Atlántico.
const URUGUAY_BOUNDS = [[-35.20, -58.60], [-29.90, -53.00]];
const URUGUAY_MIN_ZOOM = 6;
const URUGUAY_MAX_ZOOM = 17;
const TRANSPARENT_TILE_DATA_URL = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVR42mP4DwQACfsD/Ql8Z9sAAAAASUVORK5CYII=';
const BUILTIN_LAYER_DEFS = [
  {
    id: 'alerta',
    label: 'Alerta',
    category: 'Analiticas',
    provider: 'AgroClimaX',
    type: 'analytic',
    tileLayerName: 'alerta_fusion',
    minZoom: 7,
    opacityDefault: 0.84,
    zIndexPriority: 210,
    recommended: true,
  },
  {
    id: 'rgb',
    label: 'RGB',
    category: 'Analiticas',
    provider: 'Copernicus',
    type: 'analytic',
    tileLayerName: 'rgb',
    minZoom: 7,
    opacityDefault: 0.82,
    zIndexPriority: 211,
  },
  {
    id: 'ndvi',
    label: 'NDVI',
    category: 'Analiticas',
    provider: 'Copernicus',
    type: 'analytic',
    tileLayerName: 'ndvi',
    minZoom: 7,
    opacityDefault: 0.82,
    zIndexPriority: 212,
  },
  {
    id: 'ndmi',
    label: 'NDMI',
    category: 'Analiticas',
    provider: 'Copernicus',
    type: 'analytic',
    tileLayerName: 'ndmi',
    minZoom: 7,
    opacityDefault: 0.82,
    zIndexPriority: 213,
  },
  {
    id: 'ndwi',
    label: 'NDWI',
    category: 'Analiticas',
    provider: 'Copernicus',
    type: 'analytic',
    tileLayerName: 'ndwi',
    minZoom: 7,
    opacityDefault: 0.82,
    zIndexPriority: 214,
  },
  {
    id: 'savi',
    label: 'SAVI',
    category: 'Analiticas',
    provider: 'Copernicus',
    type: 'analytic',
    tileLayerName: 'savi',
    minZoom: 7,
    opacityDefault: 0.82,
    zIndexPriority: 215,
  },
  {
    id: 'sar',
    label: 'SAR VV',
    category: 'Analiticas',
    provider: 'Copernicus',
    type: 'analytic',
    tileLayerName: 'sar',
    minZoom: 7,
    opacityDefault: 0.84,
    zIndexPriority: 216,
  },
  {
    id: 'lst',
    label: 'Termal',
    category: 'Analiticas',
    provider: 'Copernicus',
    type: 'analytic',
    tileLayerName: 'lst',
    minZoom: 7,
    opacityDefault: 0.8,
    zIndexPriority: 217,
  },
  {
    id: 'judicial',
    label: 'Secciones',
    category: 'Administrativas',
    provider: 'SNIG',
    type: 'admin',
    minZoom: 7,
    opacityDefault: 0.85,
    zIndexPriority: 410,
  },
  {
    id: 'productiva',
    label: 'Predios',
    category: 'Administrativas',
    provider: 'AgroClimaX',
    type: 'admin',
    minZoom: 7,
    opacityDefault: 0.85,
    zIndexPriority: 420,
  },
  {
    id: 'hex',
    label: 'H3',
    category: 'Administrativas',
    provider: 'AgroClimaX',
    type: 'admin',
    minZoom: 7,
    opacityDefault: 0.85,
    zIndexPriority: 430,
  },
];
const LAYER_CATEGORY_ORDER = ['Analiticas', 'Suelos', 'Agua', 'Parcelas', 'Infraestructura', 'Restricciones', 'Administrativas'];
const RECOMMENDED_LAYER_IDS = ['alerta', 'coneat', 'hidrografia', 'area_inundable', 'catastro_rural', 'rutas_camineria', 'zonas_sensibles'];
const TIMELINE_WINDOW_DAYS = 365;
const TIMELINE_SPEED_PRESETS = [
  { value: 0.5, label: '0.5x', intervalMs: 1800 },
  { value: 1, label: '1x', intervalMs: 900 },
  { value: 2, label: '2x', intervalMs: 450 },
  { value: 4, label: '4x', intervalMs: 225 },
  { value: 8, label: '8x', intervalMs: 140 },
];
const TIMELINE_LAYER_IDS = new Set(BUILTIN_LAYER_DEFS.filter((layer) => layer.type === 'analytic').map((layer) => layer.id));
let farmManualDrawMode = null;
let farmManualDrawPoints = [];
let farmManualDrawMarkers = [];
let farmManualPreviewLayer = null;
let farmManualMapClickHandler = null;
let farmManualMapDblClickHandler = null;
let farmEditorContextLayer = null;
let timelineManifestRequestSeq = 0;
let timelineViewportRefreshHandle = null;
let timelinePlaybackHandle = null;

function tileUrl(layerName) {
  return buildAnalyticTileUrl(layerName);
}

function clearMarkers() {
  store.unitMarkers.forEach((marker) => store.map.removeLayer(marker));
  setStore({ unitMarkers: [] });
}

export function clearDepartmentLayer() {
  if (store.departmentsLayer) store.map.removeLayer(store.departmentsLayer);
  setStore({ departmentsLayer: null, departmentsLookup: {} });
}

function layerOpacityValue(layerId, fallback = 0.85) {
  const value = store.layerOpacities?.[layerId];
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  return fallback;
}

function getBuiltinLayerDefinition(layerId) {
  return BUILTIN_LAYER_DEFS.find((layer) => layer.id === layerId) || null;
}

function getAvailableOverlayDefinition(layerId) {
  return (store.availableOverlays || []).find((layer) => layer.id === layerId) || null;
}

function getLayerDefinition(layerId) {
  return getBuiltinLayerDefinition(layerId) || getAvailableOverlayDefinition(layerId) || null;
}

function getAllLayerDefinitions() {
  return [...BUILTIN_LAYER_DEFS, ...(store.availableOverlays || [])];
}

export function isLayerActive(layerId) {
  return Array.isArray(store.activeLayers) && store.activeLayers.includes(layerId);
}

function isTemporalLayerId(layerId) {
  return TIMELINE_LAYER_IDS.has(layerId);
}

function getActiveTemporalLayerIds() {
  return orderedActiveLayerIds((store.activeLayers || []).filter((layerId) => isTemporalLayerId(layerId)));
}

function todayIsoDate() {
  // Fecha local del navegador (ver app.js:todayIsoDate para la rationale).
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, '0');
  const d = String(now.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function parseIsoDate(value) {
  if (!value) return null;
  const parsed = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function formatIsoDate(value) {
  return value ? value.toISOString().slice(0, 10) : null;
}

function addDays(isoDate, days) {
  const parsed = parseIsoDate(isoDate);
  if (!parsed) return isoDate;
  parsed.setUTCDate(parsed.getUTCDate() + days);
  return formatIsoDate(parsed);
}

function startTimelineDate() {
  return addDays(todayIsoDate(), -(TIMELINE_WINDOW_DAYS - 1));
}

function normalizeTimelineSpeed(value) {
  const numeric = Number(value);
  return TIMELINE_SPEED_PRESETS.find((item) => item.value === numeric)?.value || 1;
}

function playbackIntervalMs() {
  return TIMELINE_SPEED_PRESETS.find((item) => item.value === normalizeTimelineSpeed(store.timelineSpeed))?.intervalMs || 900;
}

function timelineStepDaysForSpeed() {
  const speed = normalizeTimelineSpeed(store.timelineSpeed);
  if (speed >= 8) return 10;
  if (speed >= 4) return 5;
  if (speed >= 2) return 2;
  return 1;
}

function timelineWarmRunwayDays() {
  const speed = normalizeTimelineSpeed(store.timelineSpeed);
  if (speed >= 8) return 18;
  if (speed >= 4) return 12;
  if (speed >= 2) return 8;
  return 5;
}

function timelineSliderIndex(isoDate) {
  const start = parseIsoDate(startTimelineDate());
  const current = parseIsoDate(isoDate);
  if (!start || !current) return TIMELINE_WINDOW_DAYS - 1;
  return Math.max(0, Math.min(TIMELINE_WINDOW_DAYS - 1, Math.round((current.getTime() - start.getTime()) / 86400000)));
}

function isoDateFromSliderIndex(index) {
  const start = parseIsoDate(startTimelineDate());
  if (!start) return todayIsoDate();
  start.setUTCDate(start.getUTCDate() + Number(index || 0));
  return formatIsoDate(start);
}

function timelineViewportContext() {
  if (!store.map) return { bbox: null, zoom: null, key: 'no-map' };
  const bounds = store.map.getBounds();
  const bbox = [
    bounds.getWest(),
    bounds.getSouth(),
    bounds.getEast(),
    bounds.getNorth(),
  ].map((value) => Number(value).toFixed(4)).join(',');
  const zoom = Math.round(store.map.getZoom());
  return { bbox, zoom, key: `${bbox}|${zoom}` };
}

function currentViewportPreloadDescriptor() {
  if (store.selectedPaddockId && store.selectedFieldDetail) {
    const paddock = (store.selectedFieldDetail.paddocks || []).find((item) => item.id === store.selectedPaddockId);
    if (paddock?.aoi_unit_id) {
      return {
        scope_type: 'paddock',
        scope_ref: `paddock:${paddock.id}`,
        timeline_scope: 'unidad',
        timeline_unit_id: paddock.aoi_unit_id,
        timeline_department: store.selectedFieldDetail.department || null,
      };
    }
  }
  if (store.selectedFieldId) {
    const field = store.selectedFieldDetail || (store.farmFields || []).find((item) => item.id === store.selectedFieldId);
    if (field?.aoi_unit_id) {
      return {
        scope_type: 'field',
        scope_ref: `field:${field.id}`,
        timeline_scope: 'unidad',
        timeline_unit_id: field.aoi_unit_id,
        timeline_department: field.department || null,
      };
    }
  }
  if (store.selectedProductiveId) {
    return {
      scope_type: 'unidad',
      scope_ref: store.selectedProductiveId,
      timeline_scope: 'unidad',
      timeline_unit_id: store.selectedProductiveId,
      timeline_department: null,
    };
  }
  if (store.selectedSectionId) {
    return {
      scope_type: 'unidad',
      scope_ref: store.selectedSectionId,
      timeline_scope: 'unidad',
      timeline_unit_id: store.selectedSectionId,
      timeline_department: null,
    };
  }
  if (store.selectedHexId) {
    return {
      scope_type: 'unidad',
      scope_ref: store.selectedHexId,
      timeline_scope: 'unidad',
      timeline_unit_id: store.selectedHexId,
      timeline_department: null,
    };
  }
  if (store.selectedScope === 'departamento' && store.selectedDepartment) {
    return {
      scope_type: 'departamento',
      scope_ref: store.selectedDepartment,
      timeline_scope: 'departamento',
      timeline_unit_id: null,
      timeline_department: store.selectedDepartment,
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

async function maybeStartViewportPreload(activeTemporalIds) {
  if (!store.map || !activeTemporalIds.length) return;
  if (store.preloadVisible && !store.preloadCriticalReady) return;
  const viewport = timelineViewportContext();
  const officialIds = orderedActiveLayerIds((store.activeLayers || []).filter((layerId) => getLayerDefinition(layerId)?.type === 'official'));
  const descriptor = currentViewportPreloadDescriptor();
  const container = store.map.getContainer?.();
  const signature = [
    viewport.key,
    activeTemporalIds.join(','),
    officialIds.join(','),
    descriptor.scope_ref || descriptor.timeline_department || descriptor.scope_type,
  ].join('|');
  if (store.preloadViewportSignature === signature) return;
  setStore({ preloadViewportSignature: signature });
  try {
    const payload = await startViewportPreload({
      bbox: viewport.bbox,
      zoom: viewport.zoom,
      width: Math.max(256, Math.round(container?.clientWidth || 1024)),
      height: Math.max(256, Math.round(container?.clientHeight || 640)),
      temporal_layers: activeTemporalIds,
      official_layers: officialIds,
      ...descriptor,
      target_date: todayIsoDate(),
      history_days: 30,
    });
    if (payload?.run_key) {
      window.dispatchEvent(new CustomEvent('agroclimax:viewport-preload-started', { detail: payload }));
    }
  } catch (error) {
    console.warn('No se pudo iniciar la precarga residual del viewport:', error);
  }
}

function currentTimelineDayPayload() {
  if (!store.timelineFrames?.days?.length || !store.timelineDate) return null;
  return store.timelineFrames.days.find((day) => day.display_date === store.timelineDate) || null;
}

function currentTimelineModeLabel() {
  const day = currentTimelineDayPayload();
  if (!day) return 'Sin datos';
  const layerFrames = Object.values(day.layers || {});
  if (layerFrames.length && layerFrames.every((frame) => !frame.is_interpolated)) return 'Real';
  return 'Interpolado';
}

function currentTimelineSourceSummary() {
  const day = currentTimelineDayPayload();
  if (!day) return 'Sin capas temporales activas';
  const layerFrames = Object.values(day.layers || {});
  const sample = layerFrames.find(Boolean);
  if (!sample) return 'Sin datos';
  if (!sample.is_interpolated || !sample.secondary_source_date) {
    return `Fuente ${sample.primary_source_date}`;
  }
  return `${sample.primary_source_date} → ${sample.secondary_source_date}`;
}

function dispatchTimelineDateChange({ date, dayPayload = null, enabled = true } = {}) {
  const detail = {
    date: date || store.timelineDate || todayIsoDate(),
    enabled,
    timelineEnabled: Boolean(enabled && store.timelineEnabled),
    dayPayload,
    sourceSummary: enabled ? currentTimelineSourceSummary() : 'Timeline inactiva',
    modeLabel: enabled ? currentTimelineModeLabel() : 'Inactiva',
    _handledByStoreCallback: false,
  };
  if (typeof store.onTimelineDateChange === 'function') {
    try {
      detail._handledByStoreCallback = true;
      store.onTimelineDateChange(detail);
    } catch (error) {
      console.warn('No se pudo notificar el cambio de timeline por callback directo:', error);
    }
  }
  window.dispatchEvent(new CustomEvent('agroclimax:timeline-date-change', { detail }));
}

function setLayerError(layerId, message = '') {
  const nextErrors = { ...(store.layerErrors || {}) };
  if (message) nextErrors[layerId] = message;
  else delete nextErrors[layerId];
  setStore({ layerErrors: nextErrors });
  renderLayerMenu();
}

function tileCoordsToBBox(coords) {
  const bounds = tileToBBox(coords.z, coords.x, coords.y);
  return bounds.map((value) => Number(value).toFixed(6)).join(',');
}

function tileToBBox(z, x, y) {
  const n = 2 ** z;
  const lonMin = (x / n) * 360.0 - 180.0;
  const lonMax = ((x + 1) / n) * 360.0 - 180.0;
  const latMax = (Math.atan(Math.sinh(Math.PI * (1 - (2 * y) / n))) * 180.0) / Math.PI;
  const latMin = (Math.atan(Math.sinh(Math.PI * (1 - (2 * (y + 1)) / n))) * 180.0) / Math.PI;
  return [lonMin, latMin, lonMax, latMax];
}

function buildOfficialOverlayTileUrl(overlayId, coords, size = 256) {
  const bbox = tileCoordsToBBox(coords);
  const url = new URL(`${API_V1}/map-overlays/${encodeURIComponent(overlayId)}/tile`, window.location.origin);
  url.searchParams.set('bbox', bbox);
  url.searchParams.set('bboxSR', '4326');
  url.searchParams.set('imageSR', '4326');
  url.searchParams.set('width', String(size));
  url.searchParams.set('height', String(size));
  url.searchParams.set('format', 'image/png');
  url.searchParams.set('transparent', 'true');
  return url.toString().replace(window.location.origin, '');
}

function buildOfficialOverlayViewportUrl(definition) {
  if (!store.map) return TRANSPARENT_TILE_DATA_URL;
  if (store.map.getZoom() < (definition.minZoom || 0)) return TRANSPARENT_TILE_DATA_URL;
  const bounds = store.map.getBounds();
  const size = store.map.getSize();
  const bbox = [
    bounds.getWest(),
    bounds.getSouth(),
    bounds.getEast(),
    bounds.getNorth(),
  ].map((value) => Number(value).toFixed(6)).join(',');
  const url = new URL(`${API_V1}/map-overlays/${encodeURIComponent(definition.id)}/tile`, window.location.origin);
  url.searchParams.set('bbox', bbox);
  url.searchParams.set('bboxSR', '4326');
  url.searchParams.set('imageSR', '4326');
  url.searchParams.set('width', String(Math.max(256, Math.round(size.x || 256))));
  url.searchParams.set('height', String(Math.max(256, Math.round(size.y || 256))));
  url.searchParams.set('format', 'image/png');
  url.searchParams.set('transparent', 'true');
  return url.toString().replace(window.location.origin, '');
}

function buildAnalyticTileUrl(layerName, sourceDate = store.timelineDate || todayIsoDate(), frameRole = 'primary') {
  const params = new URLSearchParams();
  if (sourceDate) params.set('source_date', sourceDate);
  if (frameRole) params.set('frame_role', frameRole);
  // Si el usuario tiene un scope activo (depto/seccion/field), le pasamos al backend
  // para que haga early-exit/clip raster. Nacional no se envia porque es el default.
  const scope = store.clipScope;
  const ref = store.clipRef;
  if (scope && scope !== 'nacional' && scope !== 'field') {
    params.set('clip_scope', scope);
    if (ref) params.set('clip_ref', ref);
  }
  const query = params.toString();
  return `${API_BASE}/tiles/${layerName}/{z}/{x}/{y}.png${query ? `?${query}` : ''}`;
}

/**
 * Redibuja todas las capas analíticas activas con la URL actual de tiles.
 * Se llama tras un cambio de scope para que los tiles se re-pidan con
 * los nuevos query params clip_scope/clip_ref.
 */
export function redrawAllAnalyticLayers() {
  const instances = store.layerInstances || {};
  Object.entries(instances).forEach(([layerId, instance]) => {
    const def = instance?.definition;
    if (!def?.tileLayerName) return;
    const primaryUrl = buildAnalyticTileUrl(def.tileLayerName, store.timelineDate || todayIsoDate(), 'primary');
    try { instance.primaryLayer?.setUrl?.(primaryUrl, false); } catch (_) { /* noop */ }
    if (instance.secondaryLayer) {
      const secondaryUrl = buildAnalyticTileUrl(def.tileLayerName, store.timelineDate || todayIsoDate(), 'secondary');
      try { instance.secondaryLayer.setUrl(secondaryUrl, false); } catch (_) { /* noop */ }
    }
  });
}

function groupDefinitionsByCategory() {
  const groups = new Map();
  LAYER_CATEGORY_ORDER.forEach((category) => groups.set(category, []));
  getAllLayerDefinitions().forEach((definition) => {
    const category = definition.category || 'Otros';
    if (!groups.has(category)) groups.set(category, []);
    groups.get(category).push(definition);
  });
  groups.forEach((items) => items.sort((left, right) => left.label.localeCompare(right.label, 'es')));
  return groups;
}

function orderedActiveLayerIds(layerIds = store.activeLayers || []) {
  return [...layerIds].sort((leftId, rightId) => {
    const left = getLayerDefinition(leftId);
    const right = getLayerDefinition(rightId);
    return (left?.zIndexPriority || 0) - (right?.zIndexPriority || 0);
  });
}

function getLayerMenuLabel(definition) {
  const source = definition.provider ? `<span class="map-layer-source">${definition.provider}</span>` : '';
  const hint = definition.minZoom ? `<span class="map-layer-zoom">zoom ${definition.minZoom}+</span>` : '';
  return `${source}${hint}`;
}

function getLayerErrorMessage(layerId) {
  return store.layerErrors?.[layerId] || '';
}

function updateBaseTileLayerOpacity() {
  if (!store.baseTileLayer) return;
  const hasAnalytic = (store.activeLayers || []).some((layerId) => getLayerDefinition(layerId)?.type === 'analytic');
  const hasOfficial = (store.activeLayers || []).some((layerId) => getLayerDefinition(layerId)?.type === 'official');
  if (isLayerActive('coneat') && !hasAnalytic) {
    store.baseTileLayer.setOpacity(0.18);
    return;
  }
  if (hasOfficial && !hasAnalytic) {
    store.baseTileLayer.setOpacity(0.42);
    return;
  }
  store.baseTileLayer.setOpacity(0.75);
}

function ensureOfficialOverlayPane() {
  if (!store.map.getPane('officialOverlayPane')) {
    store.map.createPane('officialOverlayPane');
    store.map.getPane('officialOverlayPane').style.zIndex = 405;
  }
}

function refreshOfficialOverlayLayer(layer, definition) {
  if (!store.map || !layer) return;
  if (store.map.getZoom() < (definition.minZoom || 0)) {
    setLayerError(definition.id, `Visible desde zoom ${definition.minZoom}+`);
    if (layer.setUrl) layer.setUrl(TRANSPARENT_TILE_DATA_URL);
    return;
  }
  const bounds = store.map.getBounds();
  if (layer.setBounds) layer.setBounds(bounds);
  if (layer.setUrl) layer.setUrl(buildOfficialOverlayViewportUrl(definition));
}

function createOfficialOverlayLayerInstance(definition) {
  const initialBounds = store.map?.getBounds()
    || window.L.latLngBounds(INITIAL_VIEW.center, INITIAL_VIEW.center);
  const layer = window.L.imageOverlay(TRANSPARENT_TILE_DATA_URL, initialBounds, {
    pane: 'officialOverlayPane',
    opacity: layerOpacityValue(definition.id, Number(definition.opacityDefault || 0.85)),
    zIndex: definition.zIndexPriority || 390,
    className: `overlay-layer overlay-${definition.id}`,
  });
  const refresh = () => refreshOfficialOverlayLayer(layer, definition);
  layer.on('add', refresh);
  layer.on('add', () => {
    store.map?.on('moveend zoomend resize', refresh);
  });
  layer.on('remove', () => {
    store.map?.off('moveend zoomend resize', refresh);
  });
  layer.__refreshOfficialOverlay = refresh;
  return layer;
}

function createTemporalLeafletLayer(definition, frameRole) {
  return window.L.tileLayer(buildAnalyticTileUrl(definition.tileLayerName, store.timelineDate || todayIsoDate(), frameRole), {
    pane: 'satellitePane',
    maxZoom: 18,
    maxNativeZoom: 17,
    minZoom: definition.minZoom || 7,
    tileSize: 256,
    opacity: 0,
    zIndex: definition.zIndexPriority || 200,
    className: `analytic-layer analytic-${definition.id}`,
    updateWhenIdle: false,
    keepBuffer: 1,
    bounds: URUGUAY_BOUNDS,
    noWrap: true,
  });
}

function waitForTemporalLayerLoad(layerId, definition, layer, url, { silent = false } = {}) {
  if (layer.__timelineReadyUrl === url) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const hint = definition.minZoom ? ` visible desde zoom ${definition.minZoom}+` : '';
    const onLoad = () => {
      cleanup();
      layer.__timelineReadyUrl = url;
      setLayerError(layerId, '');
      if (!silent) hideMapStatus(`Cargando ${definition.label}${hint}...`);
      resolve();
    };
    const onError = () => {
      cleanup();
      setLayerError(layerId, `No se pudo cargar ${definition.label}`);
      if (!silent) showMapStatus(`No se pudo cargar ${definition.label}`, 2600);
      reject(new Error(`No se pudo cargar ${definition.label}`));
    };
    const cleanup = () => {
      layer.off('load', onLoad);
      layer.off('tileerror', onError);
      layer.off('error', onError);
    };
    if (!silent) showMapStatus(`Cargando ${definition.label}${hint}...`);
    layer.on('load', onLoad);
    layer.on('tileerror', onError);
    layer.on('error', onError);
    layer.setUrl(url);
  });
}

function crossfadeTemporalLayers(outgoingLayer, incomingLayer, targetOpacity, durationMs = 220) {
  return new Promise((resolve) => {
    if (durationMs <= 0) {
      if (outgoingLayer) outgoingLayer.setOpacity(0);
      incomingLayer.setOpacity(targetOpacity);
      resolve();
      return;
    }
    if (!outgoingLayer) {
      incomingLayer.setOpacity(targetOpacity);
      resolve();
      return;
    }
    const start = performance.now();
    const animate = (now) => {
      const progress = Math.min(1, (now - start) / durationMs);
      incomingLayer.setOpacity(targetOpacity * progress);
      outgoingLayer.setOpacity(targetOpacity * (1 - progress));
      if (progress < 1) {
        window.requestAnimationFrame(animate);
        return;
      }
      outgoingLayer.setOpacity(0);
      incomingLayer.setOpacity(targetOpacity);
      resolve();
    };
    window.requestAnimationFrame(animate);
  });
}

function createTemporalLayerController(definition) {
  const group = window.L.layerGroup();
  const primaryLayer = createTemporalLeafletLayer(definition, 'primary');
  const secondaryLayer = createTemporalLeafletLayer(definition, 'secondary');
  group.addLayer(primaryLayer);
  group.addLayer(secondaryLayer);

  const controller = {
    __kind: 'temporal-analytic',
    definition,
    group,
    primaryLayer,
    secondaryLayer,
    visibleLayer: null,
    bufferLayer: primaryLayer,
    currentDate: null,
    currentUrl: null,
    preloadedDate: null,
    preloadedUrl: null,
    baseOpacity: layerOpacityValue(definition.id, Number(definition.opacityDefault || 0.82)),
    addTo(map) {
      group.addTo(map);
    },
    removeFrom(map) {
      if (map.hasLayer(group)) map.removeLayer(group);
    },
    setOpacity(value) {
      this.baseOpacity = value;
      if (this.visibleLayer) {
        this.visibleLayer.setOpacity(value);
        return;
      }
      this.primaryLayer.setOpacity(value);
      this.secondaryLayer.setOpacity(0);
    },
    setZIndex(value) {
      this.primaryLayer.setZIndex(value);
      this.secondaryLayer.setZIndex(value);
    },
    clearPrefetch() {
      this.preloadedDate = null;
      this.preloadedUrl = null;
      if (this.bufferLayer) this.bufferLayer.__timelineReadyUrl = null;
    },
    async prefetch(frame) {
      if (!frame?.available) return;
      if (frame.display_date === this.currentDate) return;
      const targetLayer = this.bufferLayer || this.primaryLayer;
      const url = buildAnalyticTileUrl(definition.tileLayerName, frame.display_date, 'primary');
      if (this.preloadedDate === frame.display_date && this.preloadedUrl === url) return;
      try {
        await waitForTemporalLayerLoad(definition.id, definition, targetLayer, url, { silent: true });
        this.preloadedDate = frame.display_date;
        this.preloadedUrl = url;
      } catch (error) {
        console.warn(`No se pudo precargar ${definition.label}:`, error);
      }
    },
    async show(frame, { animate = true } = {}) {
      if (!frame?.available) return;
      const targetDate = frame.display_date;
      const targetUrl = buildAnalyticTileUrl(definition.tileLayerName, targetDate, 'primary');
      if (this.currentDate === targetDate && this.currentUrl === targetUrl) {
        if (this.visibleLayer) this.visibleLayer.setOpacity(this.baseOpacity);
        return;
      }
      const targetLayer = this.bufferLayer || this.primaryLayer;
      if (!(this.preloadedDate === targetDate && this.preloadedUrl === targetUrl)) {
        await waitForTemporalLayerLoad(definition.id, definition, targetLayer, targetUrl, { silent: false });
      }
      const outgoingLayer = this.visibleLayer;
      await crossfadeTemporalLayers(outgoingLayer, targetLayer, this.baseOpacity, animate ? 220 : 0);
      this.visibleLayer = targetLayer;
      this.bufferLayer = outgoingLayer || (targetLayer === this.primaryLayer ? this.secondaryLayer : this.primaryLayer);
      this.currentDate = targetDate;
      this.currentUrl = targetUrl;
      this.preloadedDate = null;
      this.preloadedUrl = null;
    },
  };

  return controller;
}

function attachLayerEvents(layerId, definition, layer) {
  layer.on('loading', () => {
    const hint = definition.minZoom ? ` visible desde zoom ${definition.minZoom}+` : '';
    showMapStatus(`Cargando ${definition.label}${hint}...`);
  });
  layer.on('load', () => {
    setLayerError(layerId, '');
    hideMapStatus();
  });
  const handleError = () => {
    setLayerError(layerId, `No se pudo cargar ${definition.label}`);
    showMapStatus(`No se pudo cargar ${definition.label}`, 2600);
  };
  layer.on('tileerror', handleError);
  layer.on('error', handleError);
}

function removeLayerInstanceFromMap(instance) {
  if (!store.map || !instance) return;
  if (instance.__kind === 'temporal-analytic') {
    instance.removeFrom(store.map);
    return;
  }
  if (store.map.hasLayer(instance)) store.map.removeLayer(instance);
}

function addLayerInstanceToMap(instance) {
  if (!store.map || !instance) return;
  if (instance.__kind === 'temporal-analytic') {
    instance.addTo(store.map);
    return;
  }
  instance.addTo(store.map);
}

function applyLayerInstanceOpacity(layerId, definition, instance) {
  const nextOpacity = layerOpacityValue(layerId, Number(definition.opacityDefault || 0.85));
  if (instance?.__kind === 'temporal-analytic') {
    instance.setOpacity(nextOpacity);
    return;
  }
  if (instance?.setOpacity) instance.setOpacity(nextOpacity);
}

function applyLayerInstanceZIndex(definition, instance) {
  if (instance?.__kind === 'temporal-analytic') {
    instance.setZIndex(definition.zIndexPriority || 200);
    return;
  }
  if (instance?.setZIndex) instance.setZIndex(definition.zIndexPriority || 200);
}

function renderActiveTileLayers() {
  if (!store.map) return;
  ensureOfficialOverlayPane();
  const nextInstances = { ...(store.layerInstances || {}) };
  const activeIds = new Set(store.activeLayers || []);
  const activeTileIds = orderedActiveLayerIds().filter((layerId) => {
    const definition = getLayerDefinition(layerId);
    return definition?.type === 'analytic' || definition?.type === 'official';
  });

  Object.entries(nextInstances).forEach(([layerId, instance]) => {
    if (activeIds.has(layerId)) return;
    removeLayerInstanceFromMap(instance);
    delete nextInstances[layerId];
  });

  activeTileIds.forEach((layerId) => {
    const definition = getLayerDefinition(layerId);
    if (!definition) return;
    let instance = nextInstances[layerId];
    let createdNow = false;
    if (!instance) {
      instance = definition.type === 'official'
        ? createOfficialOverlayLayerInstance(definition)
        : createTemporalLayerController(definition);
      if (definition.type === 'official') attachLayerEvents(layerId, definition, instance);
      addLayerInstanceToMap(instance);
      nextInstances[layerId] = instance;
      createdNow = true;
    }
    applyLayerInstanceOpacity(layerId, definition, instance);
    applyLayerInstanceZIndex(definition, instance);
    if (definition.type === 'official' && typeof instance.__refreshOfficialOverlay === 'function') {
      instance.__refreshOfficialOverlay();
    }
    if (
      definition.type === 'analytic'
      && createdNow
      && instance.__kind === 'temporal-analytic'
      && !instance.currentDate
    ) {
      const initialDate = store.timelineDate || todayIsoDate();
      instance.show({ available: true, display_date: initialDate }, { animate: false }).catch((error) => {
        console.warn(`No se pudo mostrar la capa temporal ${definition.label}:`, error);
      });
    }
  });

  setStore({
    layerInstances: nextInstances,
    activeTileLayer: activeTileIds.length ? nextInstances[activeTileIds[activeTileIds.length - 1]] || null : null,
  });
  updateBaseTileLayerOpacity();
}

function toggleLayerMenu(open = !store.layerMenuOpen) {
  setStore({ layerMenuOpen: Boolean(open) });
  const panel = document.getElementById('map-layer-menu-panel');
  const toggle = document.getElementById('map-layer-menu-toggle');
  if (panel) panel.hidden = !store.layerMenuOpen;
  if (toggle) toggle.setAttribute('aria-expanded', store.layerMenuOpen ? 'true' : 'false');
}

function renderLayerMenu() {
  const groupsNode = document.getElementById('map-layer-menu-groups');
  if (!groupsNode) return;
  const groups = groupDefinitionsByCategory();
  const html = [];
  groups.forEach((items, category) => {
    if (!items.length) return;
    html.push(`<section class="map-layer-group"><h4 class="map-layer-group-title">${category}</h4>`);
    items.forEach((definition) => {
      const active = isLayerActive(definition.id);
      const opacity = Math.round(layerOpacityValue(definition.id, Number(definition.opacityDefault || 0.85)) * 100);
      const error = getLayerErrorMessage(definition.id);
      html.push(`
        <label class="map-layer-item${active ? ' is-active' : ''}">
          <span class="map-layer-item-main">
            <input type="checkbox" class="map-layer-checkbox" data-layer-id="${definition.id}" ${active ? 'checked' : ''}>
            <span class="map-layer-copy">
              <span class="map-layer-name">${definition.label}</span>
              <span class="map-layer-meta">${getLayerMenuLabel(definition)}</span>
              ${error ? `<span class="map-layer-error">${error}</span>` : ''}
            </span>
          </span>
          ${active ? `<span class="map-layer-opacity"><input type="range" min="15" max="100" value="${opacity}" data-opacity-layer="${definition.id}"><span>${opacity}%</span></span>` : ''}
        </label>
      `);
    });
    html.push('</section>');
  });
  groupsNode.innerHTML = html.join('');
  toggleLayerMenu(store.layerMenuOpen);
}

function ensureLayerControlEvents() {
  const toggle = document.getElementById('map-layer-menu-toggle');
  const panel = document.getElementById('map-layer-menu-panel');
  const groupsNode = document.getElementById('map-layer-menu-groups');
  const clearBtn = document.getElementById('map-layer-clear');
  const recommendedBtn = document.getElementById('map-layer-recommended');
  const resetBtn = document.getElementById('map-layer-reset-view');
  if (toggle && !toggle.dataset.bound) {
    toggle.dataset.bound = 'true';
    toggle.addEventListener('click', () => toggleLayerMenu());
  }
  if (groupsNode && !groupsNode.dataset.bound) {
    groupsNode.dataset.bound = 'true';
    groupsNode.addEventListener('change', async (event) => {
      const checkbox = event.target.closest('[data-layer-id]');
      if (!checkbox) return;
      await toggleMapLayer(checkbox.dataset.layerId, checkbox.checked);
    });
    groupsNode.addEventListener('input', (event) => {
      const slider = event.target.closest('[data-opacity-layer]');
      if (!slider) return;
      const value = Math.max(0.15, Math.min(1, Number(slider.value) / 100));
      setLayerOpacityValue(slider.dataset.opacityLayer, value);
    });
  }
  if (clearBtn && !clearBtn.dataset.bound) {
    clearBtn.dataset.bound = 'true';
    clearBtn.addEventListener('click', () => clearAllMapLayers());
  }
  if (recommendedBtn && !recommendedBtn.dataset.bound) {
    recommendedBtn.dataset.bound = 'true';
    recommendedBtn.addEventListener('click', () => applyRecommendedLayers());
  }
  if (resetBtn && !resetBtn.dataset.bound) {
    resetBtn.dataset.bound = 'true';
    resetBtn.addEventListener('click', () => restoreMapInitialView());
  }
  if (panel && !panel.dataset.bound) {
    panel.dataset.bound = 'true';
    panel.addEventListener('click', (event) => event.stopPropagation());
  }
}

function renderTimelineControls() {
  const root = document.getElementById('map-timeline');
  const slider = document.getElementById('map-timeline-slider');
  const playButton = document.getElementById('map-timeline-play');
  const speed = document.getElementById('map-timeline-speed');
  const dateLabel = document.getElementById('map-timeline-date');
  const modeLabel = document.getElementById('map-timeline-mode');
  const sourceLabel = document.getElementById('map-timeline-source');
  const statusLabel = document.getElementById('map-timeline-status');
  if (!root || !slider || !playButton || !speed) return;

  const enabled = Boolean(store.timelineEnabled);
  root.classList.toggle('is-disabled', !enabled);
  slider.disabled = !enabled || store.timelineLoading;
  playButton.disabled = !enabled || store.timelineLoading;
  speed.disabled = !enabled;
  slider.min = '0';
  slider.max = String(TIMELINE_WINDOW_DAYS - 1);
  slider.value = String(timelineSliderIndex(store.timelineDate || todayIsoDate()));
  speed.innerHTML = TIMELINE_SPEED_PRESETS
    .map((preset) => `<option value="${preset.value}" ${normalizeTimelineSpeed(store.timelineSpeed) === preset.value ? 'selected' : ''}>${preset.label}</option>`)
    .join('');
  playButton.textContent = store.timelinePlaying ? 'Pause' : 'Play';
  dateLabel.textContent = store.timelineDate || todayIsoDate();
  modeLabel.textContent = enabled ? currentTimelineModeLabel() : 'Inactiva';
  sourceLabel.textContent = enabled ? currentTimelineSourceSummary() : 'Activa una capa temporal para navegar';
  if (store.timelineLoading) statusLabel.textContent = 'Cargando timeline...';
  else if (store.timelineBuffering) statusLabel.textContent = 'Buffering...';
  else if (!enabled) statusLabel.textContent = 'Sin capas temporales activas';
  else statusLabel.textContent = 'Lista';
  renderTimelineEventDots();
}

function currentTimelineEventScope() {
  const department = store.selectedDepartment || null;
  const unitId = store.selectedUnitId || store.selectedProductiveId || store.selectedSectionId || store.selectedHexId || null;
  return { department, unitId };
}

function timelineEventsCacheKey(scope) {
  return `${scope.department || 'nacional'}::${scope.unitId || 'all'}`;
}

export async function ensureTimelineEventsLoaded() {
  const scope = currentTimelineEventScope();
  const key = timelineEventsCacheKey(scope);
  if (store.timelineEventsKey === key && Array.isArray(store.timelineEvents) && store.timelineEvents.length) return store.timelineEvents;
  if (store.timelineEventsLoading) return store.timelineEvents || [];
  setStore({ timelineEventsLoading: true });
  try {
    const payload = await fetchNotificationEvents({ department: scope.department, unitId: scope.unitId, limit: 100 });
    const events = Array.isArray(payload?.datos) ? payload.datos : [];
    setStore({ timelineEvents: events, timelineEventsKey: key, timelineEventsLoading: false });
    renderTimelineEventDots();
    return events;
  } catch (error) {
    console.warn('No se pudieron cargar eventos de timeline:', error);
    setStore({ timelineEvents: [], timelineEventsKey: key, timelineEventsLoading: false });
    return [];
  }
}

function renderTimelineEventDots() {
  const controls = document.querySelector('.map-timeline-controls');
  const slider = document.getElementById('map-timeline-slider');
  if (!controls || !slider) return;
  let track = controls.querySelector('.timeline-events-track');
  if (!track) {
    track = document.createElement('div');
    track.className = 'timeline-events-track';
    // Insertar justo despues del slider
    slider.insertAdjacentElement('afterend', track);
  }
  const events = Array.isArray(store.timelineEvents) ? store.timelineEvents : [];
  if (!events.length) { track.innerHTML = ''; return; }
  const startIdx = 0;
  const endIdx = TIMELINE_WINDOW_DAYS - 1;
  const start = parseIsoDate(startTimelineDate());
  if (!start) { track.innerHTML = ''; return; }
  const fragments = events.slice(0, 60).map((evt) => {
    const rawDate = evt?.alert_event?.fecha || evt?.created_at || evt?.observed_at;
    if (!rawDate) return '';
    const when = new Date(rawDate);
    if (Number.isNaN(when.getTime())) return '';
    const dayDiff = Math.round((when.getTime() - start.getTime()) / 86400000);
    if (dayDiff < startIdx || dayDiff > endIdx) return '';
    const pct = (dayDiff / endIdx) * 100;
    const severity = evt?.state || evt?.alert_event?.nivel_nombre || evt?.nivel_nombre || 'Normal';
    const title = evt?.title || evt?.alert_event?.descripcion || evt?.body || severity;
    const dateStr = when.toISOString().slice(0, 10);
    return `<span class="timeline-event" style="left:${pct.toFixed(2)}%" data-severity="${escapeInlineLabel(severity)}" data-date="${dateStr}" title="${escapeInlineLabel(`${dateStr} — ${title}`)}"><span class="timeline-event-tooltip">${escapeInlineLabel(`${dateStr} · ${severity}`)}</span></span>`;
  }).join('');
  track.innerHTML = fragments;
  // Click handler: saltar a la fecha
  track.querySelectorAll('.timeline-event').forEach((dot) => {
    dot.addEventListener('click', () => {
      const targetDate = dot.dataset.date;
      if (!targetDate) return;
      if (typeof store.onTimelineDateChange === 'function') {
        store.onTimelineDateChange({ date: targetDate, enabled: true });
      }
      setStore({ timelineDate: targetDate, timelineEnabled: true });
      renderTimelineControls();
    });
  });
}

function clearTimelinePlayback() {
  if (timelinePlaybackHandle) {
    window.clearTimeout(timelinePlaybackHandle);
    timelinePlaybackHandle = null;
  }
}

function stopTimelinePlayback() {
  clearTimelinePlayback();
  setStore({ timelinePlaying: false, timelineBuffering: false });
  renderTimelineControls();
}

function layerFrameIsAvailable(frame = null) {
  if (!frame) return false;
  if (frame.available === false) return false;
  if (frame.availability === 'missing') return false;
  return true;
}

function dayIsPlayable(dayPayload, activeTemporalIds) {
  if (!dayPayload || !activeTemporalIds.length) return false;
  return activeTemporalIds.every((layerId) => layerFrameIsAvailable(dayPayload.layers?.[layerId]));
}

function frameVisualSignature(dayPayload, activeTemporalIds) {
  if (!dayPayload || !activeTemporalIds.length) return 'none';
  return activeTemporalIds.map((layerId) => {
    const frame = dayPayload.layers?.[layerId] || {};
    const availability = frame.availability || (frame.available ? 'available' : 'missing');
    const primary = frame.primary_source_date || 'none';
    const secondary = frame.secondary_source_date || 'none';
    const blend = Number(frame.blend_weight || 0).toFixed(2);
    return [layerId, availability, primary, secondary, blend, frame.label || ''].join(':');
  }).join('|');
}

function findNextTimelineDay(
  currentDate,
  activeTemporalIds,
  {
    preferWarm = true,
    requireWarm = false,
    minStepDays = 1,
    avoidSameSignature = false,
  } = {},
) {
  const days = store.timelineFrames?.days || [];
  if (!days.length || !activeTemporalIds.length) return null;
  const currentIndex = Math.max(0, days.findIndex((day) => day.display_date === currentDate));
  const currentDay = days[currentIndex] || null;
  const currentSignature = avoidSameSignature ? frameVisualSignature(currentDay, activeTemporalIds) : null;
  const orderedCandidates = [];
  for (let offset = 1; offset <= days.length; offset += 1) {
    orderedCandidates.push({
      day: days[(currentIndex + offset) % days.length],
      offset,
    });
  }
  const playableCandidates = orderedCandidates.filter((candidate) => dayIsPlayable(candidate.day, activeTemporalIds));
  if (!playableCandidates.length) return null;
  const stepFiltered = playableCandidates.filter((candidate) => candidate.offset >= Math.max(1, Number(minStepDays || 1)));
  const signatureFiltered = avoidSameSignature && currentSignature
    ? stepFiltered.filter((candidate) => frameVisualSignature(candidate.day, activeTemporalIds) !== currentSignature)
    : stepFiltered;
  const candidatePool = signatureFiltered.length ? signatureFiltered : (stepFiltered.length ? stepFiltered : playableCandidates);
  if (!preferWarm) return candidatePool[0]?.day || null;
  const warmCandidate = candidatePool.find((candidate) => isWarmFrame(candidate.day, candidate.day.display_date, activeTemporalIds)) || null;
  if (warmCandidate) return warmCandidate.day;
  if (requireWarm) return null;
  return candidatePool[0]?.day || null;
}

function scheduleTimelinePlayback() {
  clearTimelinePlayback();
  if (!store.timelinePlaying) return;
  timelinePlaybackHandle = window.setTimeout(async () => {
    if (!store.timelinePlaying) return;
    if (store.timelineLoading || store.timelineBuffering) {
      scheduleTimelinePlayback();
      return;
    }
    const days = store.timelineFrames?.days || [];
    if (!days.length) {
      stopTimelinePlayback();
      return;
    }
    const activeTemporalIds = getActiveTemporalLayerIds();
    const nextDay = findNextTimelineDay(store.timelineDate, activeTemporalIds, {
      preferWarm: true,
      requireWarm: true,
      minStepDays: timelineStepDaysForSpeed(),
      avoidSameSignature: true,
    });
    if (!nextDay) {
      maybeWarmTimelineWindow(store.timelineDate || todayIsoDate(), { force: true }).catch((error) => {
        console.warn('No se pudo recalentar la timeline durante playback:', error);
      });
      scheduleTimelinePlayback();
      return;
    }
    await setTimelineDate(nextDay.display_date, { animate: true, fromPlayback: true });
    scheduleTimelinePlayback();
  }, playbackIntervalMs());
}

async function preloadTimelineNeighbors(currentDate) {
  const days = store.timelineFrames?.days || [];
  if (!days.length) return;
  const currentIndex = Math.max(0, days.findIndex((day) => day.display_date === currentDate));
  const runway = timelineWarmRunwayDays();
  const targets = days.slice(currentIndex + 1, currentIndex + 1 + runway);
  if (!targets.length) return;
  await Promise.all(
    getActiveTemporalLayerIds().map(async (layerId) => {
      const controller = store.layerInstances?.[layerId];
      if (!controller || controller.__kind !== 'temporal-analytic') return;
      for (const targetDay of targets) {
        await controller.prefetch({ ...(targetDay.layers?.[layerId] || {}), display_date: targetDay.display_date });
      }
    }),
  );
}

async function maybeWarmTimelineWindow(anchorDate, { force = false } = {}) {
  if (!store.map || !store.timelineEnabled) return;
  const activeTemporalIds = getActiveTemporalLayerIds();
  if (!activeTemporalIds.length) return;
  const viewport = timelineViewportContext();
  const runway = timelineWarmRunwayDays();
  const dateFrom = anchorDate;
  const availableDays = store.timelineFrames?.days || [];
  const anchorIndex = Math.max(0, availableDays.findIndex((day) => day.display_date === anchorDate));
  const clampedDateTo = availableDays[Math.min(availableDays.length - 1, anchorIndex + runway)]?.display_date || addDays(anchorDate, runway);
  const dateTo = clampedDateTo;
  const signature = [viewport.key, activeTemporalIds.join(','), dateFrom, dateTo, normalizeTimelineSpeed(store.timelineSpeed)].join('|');
  if (!force && store.timelineWarmSignature === signature) return;
  setStore({ timelineWarmSignature: signature });
  const mapNode = document.getElementById('map');
  const descriptor = currentViewportPreloadDescriptor();
  try {
    await startTimelineWindowPreload({
      bbox: viewport.bbox,
      zoom: viewport.zoom,
      width: Math.max(256, Math.round(mapNode?.clientWidth || 1024)),
      height: Math.max(256, Math.round(mapNode?.clientHeight || 640)),
      temporal_layers: activeTemporalIds,
      scope_type: descriptor.scope_type,
      scope_ref: descriptor.scope_ref,
      timeline_scope: descriptor.timeline_scope,
      timeline_unit_id: descriptor.timeline_unit_id,
      timeline_department: descriptor.timeline_department,
      date_from: dateFrom,
      date_to: dateTo,
      history_days: 30,
    });
  } catch (error) {
    console.warn('No se pudo lanzar la precarga adelantada de la timeline:', error);
  }
}

function isWarmFrame(dayPayload, targetDate, activeTemporalIds) {
  return activeTemporalIds.every((layerId) => {
    const controller = store.layerInstances?.[layerId];
    const frame = dayPayload.layers?.[layerId] || {};
    if (controller?.__kind === 'temporal-analytic') {
      if (controller.currentDate === targetDate) return true;
      if (controller.preloadedDate === targetDate) return true;
    }
    return Boolean(frame.warm_available || frame.cache_status === 'ready');
  });
}

async function setTimelineDate(targetDate, { animate = true, fromPlayback = false } = {}) {
  if (!store.timelineEnabled || !store.timelineFrames?.days?.length) return;
  const dayPayload = store.timelineFrames.days.find((day) => day.display_date === targetDate);
  if (!dayPayload) return;
  const activeTemporalIds = getActiveTemporalLayerIds();
  if (!dayIsPlayable(dayPayload, activeTemporalIds)) {
    if (fromPlayback) {
      const nextDay = findNextTimelineDay(targetDate, activeTemporalIds, {
        preferWarm: true,
        requireWarm: true,
        minStepDays: timelineStepDaysForSpeed(),
        avoidSameSignature: true,
      });
      if (nextDay && nextDay.display_date !== targetDate) {
        await setTimelineDate(nextDay.display_date, { animate, fromPlayback: true });
        return;
      }
      maybeWarmTimelineWindow(targetDate, { force: true }).catch((error) => {
        console.warn('No se pudo recalentar la timeline tras saltar un frame vacio:', error);
      });
      stopTimelinePlayback();
    }
    return;
  }
  const shouldBuffer = !isWarmFrame(dayPayload, targetDate, activeTemporalIds);
  if (fromPlayback && shouldBuffer) {
    const nextWarmDay = findNextTimelineDay(targetDate, activeTemporalIds, {
      preferWarm: true,
      requireWarm: true,
      minStepDays: timelineStepDaysForSpeed(),
      avoidSameSignature: true,
    });
    if (nextWarmDay && nextWarmDay.display_date !== targetDate) {
      await setTimelineDate(nextWarmDay.display_date, { animate, fromPlayback: true });
      return;
    }
    maybeWarmTimelineWindow(targetDate, { force: true }).catch((error) => {
      console.warn('No se pudo recalentar la timeline antes del siguiente frame:', error);
    });
    return;
  }
  setStore({ timelineDate: targetDate, timelineBuffering: shouldBuffer });
  renderTimelineControls();
  dispatchTimelineDateChange({ date: targetDate, dayPayload, enabled: true });
  try {
    await Promise.all(
      activeTemporalIds.map(async (layerId) => {
        const controller = store.layerInstances?.[layerId];
        if (!controller || controller.__kind !== 'temporal-analytic') return;
        const frame = dayPayload.layers?.[layerId];
        await controller.show({ ...(frame || {}), display_date: targetDate }, { animate });
      }),
    );
    setStore({ timelineBuffering: false });
    renderTimelineControls();
    preloadTimelineNeighbors(targetDate).catch((error) => {
      console.warn('No se pudo precargar la timeline vecina:', error);
    });
    maybeWarmTimelineWindow(targetDate).catch((error) => {
      console.warn('No se pudo extender la precarga adelantada de la timeline:', error);
    });
  } catch (error) {
    console.warn('No se pudo aplicar el frame temporal:', error);
    setStore({ timelineBuffering: false });
    renderTimelineControls();
    if (fromPlayback) stopTimelinePlayback();
  }
}

async function refreshTimelineManifest({ preserveDate = true } = {}) {
  const activeTemporalIds = getActiveTemporalLayerIds();
  if (!store.map || !activeTemporalIds.length) {
    stopTimelinePlayback();
    setStore({
      timelineEnabled: false,
      timelineLoading: false,
      timelineFrames: null,
      timelineManifestKey: null,
      timelineDate: store.timelineDate || todayIsoDate(),
    });
    renderTimelineControls();
    dispatchTimelineDateChange({ date: todayIsoDate(), enabled: false });
    return;
  }

  const viewport = timelineViewportContext();
  const requestedDateTo = todayIsoDate();
  const requestedDateFrom = startTimelineDate();
  const manifestKey = `${activeTemporalIds.join(',')}|${viewport.key}|${requestedDateFrom}|${requestedDateTo}`;
  if (store.timelineManifestKey === manifestKey && store.timelineFrames?.days?.length) {
    setStore({ timelineEnabled: true });
    renderTimelineControls();
    return;
  }

  const requestId = ++timelineManifestRequestSeq;
  setStore({ timelineLoading: true, timelineEnabled: true, timelineManifestKey: manifestKey });
  renderTimelineControls();
  try {
    const payload = await fetchTimelineFrames({
      layers: activeTemporalIds,
      dateFrom: requestedDateFrom,
      dateTo: requestedDateTo,
      bbox: viewport.bbox,
      zoom: viewport.zoom,
    });
    if (requestId !== timelineManifestRequestSeq) return;
    const nextDate = preserveDate && payload.days?.some((day) => day.display_date === store.timelineDate)
      ? store.timelineDate
      : (payload.date_to || requestedDateTo);
    setStore({
      timelineFrames: payload,
      timelineDate: nextDate,
      timelineLoading: false,
      timelineEnabled: true,
      timelineWindowDays: payload.total_days || TIMELINE_WINDOW_DAYS,
    });
    renderTimelineControls();
    maybeStartViewportPreload(activeTemporalIds).catch((error) => {
      console.warn('No se pudo programar la precarga residual del viewport:', error);
    });
    await setTimelineDate(nextDate, { animate: false });
  } catch (error) {
    if (requestId !== timelineManifestRequestSeq) return;
    console.warn('No se pudo cargar la timeline:', error);
    setStore({ timelineLoading: false, timelineEnabled: false, timelineFrames: null });
    stopTimelinePlayback();
    renderTimelineControls();
    dispatchTimelineDateChange({ date: todayIsoDate(), enabled: false });
  }
}

function scheduleTimelineManifestRefresh({ preserveDate = true } = {}) {
  if (timelineViewportRefreshHandle) window.clearTimeout(timelineViewportRefreshHandle);
  if (store.timelinePlaying) {
    setStore({ timelineBuffering: true });
    renderTimelineControls();
  }
  Object.values(store.layerInstances || {}).forEach((instance) => {
    if (instance?.__kind === 'temporal-analytic') instance.clearPrefetch();
  });
  timelineViewportRefreshHandle = window.setTimeout(() => {
    refreshTimelineManifest({ preserveDate });
  }, 260);
}

function ensureTimelineControlEvents() {
  const playButton = document.getElementById('map-timeline-play');
  const slider = document.getElementById('map-timeline-slider');
  const speed = document.getElementById('map-timeline-speed');
  if (playButton && !playButton.dataset.bound) {
    playButton.dataset.bound = 'true';
    playButton.addEventListener('click', async () => {
      if (!store.timelineEnabled) return;
      if (store.timelinePlaying) {
        stopTimelinePlayback();
        return;
      }
      const activeTemporalIds = getActiveTemporalLayerIds();
      const playableCurrent = (store.timelineFrames?.days || []).find((day) => day.display_date === (store.timelineDate || todayIsoDate()));
      if (playableCurrent && !dayIsPlayable(playableCurrent, activeTemporalIds)) {
        const nextPlayable = findNextTimelineDay(store.timelineDate || todayIsoDate(), activeTemporalIds, {
          preferWarm: true,
          minStepDays: timelineStepDaysForSpeed(),
          avoidSameSignature: true,
        });
        if (nextPlayable) {
          setStore({ timelineDate: nextPlayable.display_date });
        }
      }
      await maybeWarmTimelineWindow(store.timelineDate || todayIsoDate(), { force: true });
      setStore({ timelinePlaying: true });
      renderTimelineControls();
      scheduleTimelinePlayback();
    });
  }
  if (slider && !slider.dataset.bound) {
    slider.dataset.bound = 'true';
    slider.addEventListener('input', async (event) => {
      const nextDate = isoDateFromSliderIndex(event.target.value);
      stopTimelinePlayback();
      await setTimelineDate(nextDate, { animate: true });
    });
  }
  if (speed && !speed.dataset.bound) {
    speed.dataset.bound = 'true';
    speed.addEventListener('change', (event) => {
      setStore({ timelineSpeed: normalizeTimelineSpeed(event.target.value) });
      renderTimelineControls();
      setStore({ timelineWarmSignature: null });
      maybeWarmTimelineWindow(store.timelineDate || todayIsoDate(), { force: true }).catch((error) => {
        console.warn('No se pudo recalentar la timeline tras cambiar la velocidad:', error);
      });
      if (store.timelinePlaying) scheduleTimelinePlayback();
    });
  }
}

function showMapStatus(message, timeoutMs = 0) {
  const loading = document.getElementById('map-tile-loading');
  if (!loading) return;
  loading.textContent = message;
  loading.style.display = 'block';
  if (timeoutMs > 0) {
    window.setTimeout(() => {
      if (loading.textContent === message) loading.style.display = 'none';
    }, timeoutMs);
  }
}

function hideMapStatus(expectedMessage = null) {
  const loading = document.getElementById('map-tile-loading');
  if (!loading) return;
  if (!expectedMessage || loading.textContent === expectedMessage) loading.style.display = 'none';
}

export function clearSectionsLayer() {
  if (store.sectionsLayer) store.map.removeLayer(store.sectionsLayer);
  setStore({ sectionsLayer: null, sectionsLookup: {} });
}

export function clearProductiveLayer() {
  if (store.productiveLayer) store.map.removeLayer(store.productiveLayer);
  setStore({ productiveLayer: null, productiveLookup: {} });
}

export function clearHexLayer() {
  if (store.hexLayer) store.map.removeLayer(store.hexLayer);
  setStore({ hexLayer: null, hexLookup: {} });
}

function clearFarmGuideLayer() {
  if (store.farmGuideLayer) store.map.removeLayer(store.farmGuideLayer);
  setStore({ farmGuideLayer: null });
}

function clearFarmFieldsLayer() {
  if (store.farmFieldsLayer) store.map.removeLayer(store.farmFieldsLayer);
  setStore({ farmFieldsLayer: null, farmFieldsLookup: {} });
}

function clearFarmFieldLabelsLayer() {
  if (store.farmFieldLabelsLayer) store.map.removeLayer(store.farmFieldLabelsLayer);
  setStore({ farmFieldLabelsLayer: null });
}

function clearFarmPaddocksLayer() {
  if (store.farmPaddocksLayer) store.map.removeLayer(store.farmPaddocksLayer);
  setStore({ farmPaddocksLayer: null, farmPaddocksLookup: {} });
}

function clearFarmPaddockLabelsLayer() {
  if (store.farmPaddockLabelsLayer) store.map.removeLayer(store.farmPaddockLabelsLayer);
  setStore({ farmPaddockLabelsLayer: null });
}

function disableFarmDrawModes() {
  if (!store.map?.pm) return;
  store.map.pm.disableDraw('Polygon');
  store.map.pm.disableGlobalEditMode();
  store.map.pm.disableGlobalDragMode();
  store.map.pm.disableGlobalRemovalMode();
}

function setLayerInteractivity(layerGroup, enabled) {
  if (!layerGroup?.eachLayer) return;
  layerGroup.eachLayer((layer) => {
    if (layer.closePopup && !enabled) layer.closePopup();
    if (layer.getElement) {
      const element = layer.getElement();
      if (element) {
        element.style.pointerEvents = enabled ? 'auto' : 'none';
      }
    }
    if (layer.options) {
      layer.options.interactive = enabled;
    }
  });
}

function syncFarmOverlayInteractivity(enabled) {
  setLayerInteractivity(store.farmGuideLayer, enabled);
  setLayerInteractivity(store.farmFieldsLayer, enabled);
  setLayerInteractivity(store.farmFieldLabelsLayer, enabled);
  setLayerInteractivity(store.farmPaddocksLayer, enabled);
  setLayerInteractivity(store.farmPaddockLabelsLayer, enabled);
}

function syncOperationalOverlayInteractivity(enabled) {
  setLayerInteractivity(store.departmentsLayer, enabled);
  setLayerInteractivity(store.sectionsLayer, enabled);
  setLayerInteractivity(store.productiveLayer, enabled);
  setLayerInteractivity(store.hexLayer, enabled);
}

function setFarmOverlayVisibility(visible) {
  if (!store.map) return;
  const layers = [
    store.farmGuideLayer,
    store.farmFieldsLayer,
    store.farmFieldLabelsLayer,
    store.farmPaddocksLayer,
    store.farmPaddockLabelsLayer,
  ];
  layers.forEach((layerGroup) => {
    if (!layerGroup) return;
    const onMap = store.map.hasLayer(layerGroup);
    if (visible && !onMap) {
      layerGroup.addTo(store.map);
    }
    if (!visible && onMap) {
      store.map.removeLayer(layerGroup);
    }
  });
}

function clearFarmEditorContextLayer() {
  if (farmEditorContextLayer && store.map?.hasLayer(farmEditorContextLayer)) {
    store.map.removeLayer(farmEditorContextLayer);
  }
  farmEditorContextLayer = null;
}

function showFarmEditorContext(geometry) {
  if (!store.map || !geometry) return;
  clearFarmEditorContextLayer();
  farmEditorContextLayer = window.L.geoJSON(
    { type: 'Feature', geometry, properties: {} },
    {
      pane: 'farmGuidePane',
      interactive: false,
      style: {
        color: '#7dc7ff',
        weight: 2.4,
        opacity: 0.95,
        dashArray: '8 6',
        fillColor: '#4a90d9',
        fillOpacity: 0.04,
      },
    },
  ).addTo(store.map);
}

function clearFarmManualDrawArtifacts() {
  farmManualDrawMarkers.forEach((marker) => {
    if (store.map?.hasLayer(marker)) store.map.removeLayer(marker);
  });
  farmManualDrawMarkers = [];
  if (farmManualPreviewLayer && store.map?.hasLayer(farmManualPreviewLayer)) {
    store.map.removeLayer(farmManualPreviewLayer);
  }
  farmManualPreviewLayer = null;
  farmManualDrawPoints = [];
}

function stopFarmManualDrawing() {
  if (store.map && farmManualMapClickHandler) store.map.off('click', farmManualMapClickHandler);
  if (store.map && farmManualMapDblClickHandler) store.map.off('dblclick', farmManualMapDblClickHandler);
  farmManualMapClickHandler = null;
  farmManualMapDblClickHandler = null;
  farmManualDrawMode = null;
  clearFarmManualDrawArtifacts();
}

function setFarmEditorMode(active) {
  if (!store.map) return;
  if (active) {
    if (store.map.dragging?.disable) store.map.dragging.disable();
    if (store.map.boxZoom?.disable) store.map.boxZoom.disable();
    if (store.map.touchZoom?.disable) store.map.touchZoom.disable();
    if (store.map.scrollWheelZoom?.disable) store.map.scrollWheelZoom.disable();
    store.map.getContainer().style.cursor = 'crosshair';
    return;
  }
  if (store.map.dragging?.enable) store.map.dragging.enable();
  if (store.map.boxZoom?.enable) store.map.boxZoom.enable();
  if (store.map.touchZoom?.enable) store.map.touchZoom.enable();
  if (store.map.scrollWheelZoom?.enable) store.map.scrollWheelZoom.enable();
  store.map.getContainer().style.cursor = '';
}

function drawFarmManualPreview(pathOptions) {
  if (!store.map) return;
  if (farmManualPreviewLayer && store.map.hasLayer(farmManualPreviewLayer)) {
    store.map.removeLayer(farmManualPreviewLayer);
  }
  if (farmManualDrawPoints.length < 2) {
    farmManualPreviewLayer = null;
    return;
  }
  farmManualPreviewLayer = window.L.polygon(
    farmManualDrawPoints.map((point) => [point.lat, point.lng]),
    {
      pane: 'farmDraftPane',
      ...pathOptions,
      dashArray: '6 4',
    },
  ).addTo(store.map);
  if (farmManualPreviewLayer.bringToFront) farmManualPreviewLayer.bringToFront();
}

function finalizeFarmManualPolygonCreate(mode, pathOptions, handleDraftChange, onComplete = null) {
  if (farmManualDrawMode !== mode || farmManualDrawPoints.length < 3) return false;
  const coordinates = farmManualDrawPoints.map((point) => [point.lng, point.lat]);
  coordinates.push(coordinates[0]);
  const geometry = { type: 'Polygon', coordinates: [coordinates] };
  const draftLayer = window.L.geoJSON(
    { type: 'Feature', geometry, properties: {} },
    {
      pane: 'farmDraftPane',
      style: pathOptions,
    },
  ).getLayers()[0];
  stopFarmManualDrawing();
  if (!draftLayer) return false;
  draftLayer.addTo(store.map);
  if (draftLayer.bringToFront) draftLayer.bringToFront();
  setStore({
    farmDraftLayer: draftLayer,
    farmDraftType: mode,
    farmEditorActive: true,
    farmDraftChangeHandler: handleDraftChange,
    farmDraftCreateHandler: null,
  });
  wireDraftLayer(draftLayer, handleDraftChange);
  fitLayerBounds(draftLayer, true, 16);
  if (typeof onComplete === 'function') {
    onComplete(geometry, draftLayer);
  }
  return true;
}

function isCloseToFirstFarmManualPoint(event, tolerancePx = 14) {
  if (!store.map || farmManualDrawPoints.length < 3) return false;
  const firstPoint = farmManualDrawPoints[0];
  if (!firstPoint || !event?.latlng) return false;
  const firstPixel = store.map.latLngToContainerPoint(firstPoint);
  const clickPixel = store.map.latLngToContainerPoint(event.latlng);
  return firstPixel.distanceTo(clickPixel) <= tolerancePx;
}

function startFarmManualPolygonCreate(mode, pathOptions, handleDraftChange, onComplete = null) {
  if (!store.map) return false;
  stopFarmManualDrawing();
  farmManualDrawMode = mode;

  farmManualMapClickHandler = (event) => {
    if (farmManualDrawMode !== mode) return;
    if (isCloseToFirstFarmManualPoint(event)) {
      if (window.L?.DomEvent && event?.originalEvent) {
        window.L.DomEvent.stop(event.originalEvent);
      }
      finalizeFarmManualPolygonCreate(mode, pathOptions, handleDraftChange, onComplete);
      return;
    }
    farmManualDrawPoints.push(event.latlng);
    const marker = window.L.circleMarker(event.latlng, {
      pane: 'farmDraftPane',
      radius: 4,
      color: pathOptions.color,
      fillColor: '#ffffff',
      fillOpacity: 1,
      weight: 2,
      bubblingMouseEvents: false,
    }).addTo(store.map);
    farmManualDrawMarkers.push(marker);
    drawFarmManualPreview(pathOptions);
  };

  farmManualMapDblClickHandler = () => {
    finalizeFarmManualPolygonCreate(mode, pathOptions, handleDraftChange, onComplete);
  };

  store.map.on('click', farmManualMapClickHandler);
  store.map.on('dblclick', farmManualMapDblClickHandler);
  return true;
}

function stopEditorLayerClick(event) {
  if (!store.farmEditorActive) return false;
  if (event?.originalEvent && window.L?.DomEvent) {
    window.L.DomEvent.stop(event.originalEvent);
  }
  return true;
}

function recreateDraftLayer(layer, pathOptions) {
  const geometry = layer?.toGeoJSON?.()?.geometry || null;
  if (!geometry) return null;
  if (layer && store.map?.hasLayer(layer)) {
    store.map.removeLayer(layer);
  }
  const draftLayer = window.L.geoJSON(
    { type: 'Feature', geometry, properties: {} },
    {
      pane: 'farmDraftPane',
      style: pathOptions,
    },
  ).getLayers()[0];
  if (!draftLayer) return null;
  draftLayer.addTo(store.map);
  if (draftLayer.bringToFront) draftLayer.bringToFront();
  return draftLayer;
}

export function clearFarmGeometryEditor({ preserveDraft = false } = {}) {
  if (!store.map) return;
  disableFarmDrawModes();
  stopFarmManualDrawing();
  setFarmEditorMode(false);
  clearFarmEditorContextLayer();
  setFarmOverlayVisibility(true);
  syncOperationalOverlayInteractivity(true);
  syncFarmOverlayInteractivity(true);
  if (store.farmDraftCreateHandler) {
    store.map.off('pm:create', store.farmDraftCreateHandler);
  }
  if (store.farmDraftLayer) {
    store.map.removeLayer(store.farmDraftLayer);
  }
  setStore({
    farmDraftLayer: null,
    farmDraftType: null,
    farmEditorActive: false,
    farmDraftChangeHandler: null,
    farmDraftCreateHandler: null,
    ...(preserveDraft ? {} : { fieldDraftGeometry: null, paddockDraftGeometry: null }),
  });
}

function farmGuideStyle() {
  return {
    color: '#8ad2ff',
    weight: 2,
    opacity: 0.95,
    dashArray: '8 6',
    fillColor: '#4a90d9',
    fillOpacity: 0.05,
  };
}

function farmFieldStyle(selected = false) {
  return {
    color: selected ? '#d6f5ff' : '#5db7ff',
    weight: selected ? 3.2 : 2.2,
    opacity: selected ? 1 : 0.88,
    fillColor: '#3e91d6',
    fillOpacity: selected ? 0.18 : 0.08,
  };
}

function farmPaddockStyle(selected = false) {
  return {
    color: selected ? '#ffffff' : '#8cf0be',
    weight: selected ? 2.6 : 1.8,
    opacity: 0.96,
    fillColor: '#2ecc71',
    fillOpacity: selected ? 0.14 : 0.05,
    dashArray: selected ? '' : '4 4',
  };
}

function escapeInlineLabel(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function formatAreaHa(areaHa) {
  const numeric = Number(areaHa);
  if (!Number.isFinite(numeric)) return 'N/D';
  return numeric >= 10 ? numeric.toFixed(1) : numeric.toFixed(2);
}

function farmLabelScaleForZoom(zoom) {
  if (zoom <= 8) return 0.62;
  if (zoom <= 10) return 0.74;
  if (zoom <= 12) return 0.88;
  if (zoom <= 14) return 1;
  return 1.08;
}

function applyFarmLabelScale(marker) {
  const element = marker?.getElement?.();
  if (!element || !store.map) return;
  const zoom = store.map.getZoom();
  const chip = element.querySelector('.farm-label-chip') || element;
  chip.style.setProperty('--farm-label-scale', String(farmLabelScaleForZoom(zoom)));
  chip.dataset.density = zoom <= 10 ? 'compact' : (zoom <= 13 ? 'medium' : 'extended');
}

function updateFarmLabelScales() {
  [store.farmFieldLabelsLayer, store.farmPaddockLabelsLayer].forEach((group) => {
    group?.eachLayer?.((layer) => {
      if (layer?.getLatLng && layer?.getElement) {
        applyFarmLabelScale(layer);
      }
    });
  });
}

function getFarmLabelPositionMap(kind) {
  return kind === 'field'
    ? (store.farmFieldLabelPositions || {})
    : (store.farmPaddockLabelPositions || {});
}

function setFarmLabelPosition(kind, id, latlng) {
  const current = { ...getFarmLabelPositionMap(kind) };
  if (!latlng) {
    delete current[id];
  } else {
    current[id] = { lat: latlng.lat, lng: latlng.lng };
  }
  if (kind === 'field') {
    setStore({ farmFieldLabelPositions: current });
  } else {
    setStore({ farmPaddockLabelPositions: current });
  }
}

function formatMetricValue(value, digits = 1, suffix = '', fallback = 'N/D') {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return fallback;
  return `${numeric.toFixed(digits)}${suffix}`;
}

function labelMetricsHtml(analytics) {
  if (!analytics) return '';
  const metrics = [
    ['Estado', analytics.state || 'N/D', false],
    ['Riesgo', formatMetricValue(analytics.risk_score, 1), false],
    ['Conf.', formatMetricValue(analytics.confidence_score, 1, '%'), false],
    ['Hum.', formatMetricValue(analytics.s1_humidity_mean_pct, 1, '%'), false],
    ['NDMI', formatMetricValue(analytics.s2_ndmi_mean, 3), true],
    ['SPI-30', formatMetricValue(analytics.spi_30d, 3), true],
  ];
  return `
    <div class="farm-label-metrics">
      ${metrics.map(([label, value, detailOnly]) => `
        <div class="farm-label-metric ${detailOnly ? 'detail-only' : ''}">
          <span class="farm-label-metric-label">${escapeInlineLabel(label)}</span>
          <span class="farm-label-metric-value">${escapeInlineLabel(value)}</span>
        </div>
      `).join('')}
    </div>
  `;
}

function labelHtml(title, areaHa, kind, analytics = null) {
  return `
    <div class="farm-label-chip farm-label-chip-${kind}" data-density="extended">
      <strong>${escapeInlineLabel(title)}</strong>
      <span>${escapeInlineLabel(formatAreaHa(areaHa))} ha</span>
      ${labelMetricsHtml(analytics)}
    </div>
  `;
}

function defaultFarmLabelLatLng(anchorLatLng) {
  return window.L.latLng(anchorLatLng.lat + 0.00035, anchorLatLng.lng);
}

function createFarmAnchoredLabel({ featureLayer, id, kind, title, areaHa, analytics = null }) {
  if (!store.map || !featureLayer?.getBounds) return [];
  const anchorLatLng = featureLayer.getBounds().getCenter();
  const storedPosition = getFarmLabelPositionMap(kind)?.[id];
  const markerLatLng = storedPosition
    ? window.L.latLng(storedPosition.lat, storedPosition.lng)
    : defaultFarmLabelLatLng(anchorLatLng);
  const tether = window.L.polyline([anchorLatLng, markerLatLng], {
    pane: 'farmTetherPane',
    interactive: false,
    color: kind === 'field' ? '#7dbfff' : '#9af0c7',
    weight: 1.2,
    opacity: 0.75,
    dashArray: '4 4',
  });
  const marker = window.L.marker(markerLatLng, {
    pane: 'farmLabelPane',
    draggable: true,
    autoPan: false,
    icon: window.L.divIcon({
      className: 'farm-label-marker',
      html: labelHtml(title, areaHa, kind, analytics),
      iconSize: null,
    }),
  });
  const updateTether = () => {
    tether.setLatLngs([anchorLatLng, marker.getLatLng()]);
    applyFarmLabelScale(marker);
  };
  marker.on('drag', updateTether);
  marker.on('dragend', () => {
    updateTether();
    setFarmLabelPosition(kind, id, marker.getLatLng());
  });
  marker.on('click', (event) => {
    if (window.L?.DomEvent && event?.originalEvent) {
      window.L.DomEvent.stop(event.originalEvent);
    }
  });
  updateTether();
  return [tether, marker];
}

function ensureLayerGroupOnMap(layerGroup) {
  if (!store.map || !layerGroup) return;
  if (!store.map.hasLayer(layerGroup)) {
    layerGroup.addTo(store.map);
  }
}

export function refreshFarmPrivateOverlays() {
  if (!store.map) return;
  [
    store.farmGuideLayer,
    store.farmFieldsLayer,
    store.farmFieldLabelsLayer,
    store.farmPaddocksLayer,
    store.farmPaddockLabelsLayer,
  ].forEach(ensureLayerGroupOnMap);
  if (store.farmFieldsLayer?.bringToFront) store.farmFieldsLayer.bringToFront();
  if (store.farmPaddocksLayer?.bringToFront) store.farmPaddocksLayer.bringToFront();
  store.farmFieldLabelsLayer?.eachLayer?.((layer) => {
    if (layer?.bringToFront) layer.bringToFront();
    if (layer?.setZIndexOffset) layer.setZIndexOffset(800);
  });
  store.farmPaddockLabelsLayer?.eachLayer?.((layer) => {
    if (layer?.bringToFront) layer.bringToFront();
    if (layer?.setZIndexOffset) layer.setZIndexOffset(900);
  });
  updateFarmLabelScales();
}

function farmFieldPopup(props) {
  const analytics = props.analytics || {};
  return `
    <div style="min-width:220px">
      <strong>${props.unit_name || 'Campo'}</strong><br>
      <span style="color:#9fb0c7">${props.establishment_name || 'Establecimiento'} Â· ${props.department || ''}</span><br>
      <div style="margin-top:6px">Padron: <strong>${props.padron_value || 'N/D'}</strong></div>
      <div>Area: <strong>${props.area_ha ?? 'N/D'} ha</strong></div>
      <div>Estado: <strong>${analytics.state || 'N/D'}</strong></div>
      <div>Riesgo: <strong>${formatMetricValue(analytics.risk_score, 1)}</strong></div>
      <div>Confianza: <strong>${formatMetricValue(analytics.confidence_score, 1, '%')}</strong></div>
      <div>Humedad S1: <strong>${formatMetricValue(analytics.s1_humidity_mean_pct, 1, '%')}</strong></div>
    </div>
  `;
}

function paddockPopup(props) {
  const analytics = props.analytics || {};
  return `
    <div style="min-width:200px">
      <strong>${props.name || 'Potrero'}</strong><br>
      <span style="color:#9fb0c7">Campo ${props.field_id || '-'}</span><br>
      <div style="margin-top:6px">Area: <strong>${props.area_ha ?? 'N/D'} ha</strong></div>
      <div>Estado: <strong>${analytics.state || 'N/D'}</strong></div>
      <div>Riesgo: <strong>${formatMetricValue(analytics.risk_score, 1)}</strong></div>
      <div>Confianza: <strong>${formatMetricValue(analytics.confidence_score, 1, '%')}</strong></div>
      <div>Humedad S1: <strong>${formatMetricValue(analytics.s1_humidity_mean_pct, 1, '%')}</strong></div>
    </div>
  `;
}

function fitLayerBounds(layer, fitBounds = false, maxZoom = 15) {
  if (!store.map || !fitBounds || !layer?.getBounds) return;
  store.map.fitBounds(layer.getBounds(), { padding: [28, 28], maxZoom });
}

function captureDraftGeometry(layer, handler) {
  if (!layer || typeof handler !== 'function') return;
  const geometry = layer.toGeoJSON()?.geometry || null;
  handler(geometry, layer);
}

function wireDraftLayer(layer, handler) {
  if (!layer) return;
  if (layer.pm) {
    layer.pm.enable({
      allowSelfIntersection: false,
      snappable: true,
    });
  }
  const update = () => captureDraftGeometry(layer, handler);
  layer.on('pm:edit', update);
  layer.on('pm:update', update);
  layer.on('pm:dragend', update);
  update();
}

export function fitGeojsonBounds(geojson, maxZoom = 15) {
  if (!store.map || !geojson) return;
  const layer = window.L.geoJSON(geojson);
  if (!layer.getLayers().length) return;
  store.map.fitBounds(layer.getBounds(), { padding: [28, 28], maxZoom });
}

export function setFarmGuideOnMap(feature) {
  if (!store.map) return;
  clearFarmGuideLayer();
  if (!feature) return;
  const guideLayer = window.L.geoJSON(feature, {
    pane: 'farmGuidePane',
    style: farmGuideStyle,
  }).addTo(store.map);
  setStore({ farmGuideLayer: guideLayer });
  fitGeojsonBounds(feature, 15);
}

export function setFarmFieldsOnMap(featureCollection, onFieldSelect, selectedFieldId = null) {
  if (!store.map) return;
  clearFarmFieldsLayer();
  clearFarmFieldLabelsLayer();
  const lookup = {};
  const labelLayer = window.L.layerGroup();
  const layer = window.L.geoJSON(featureCollection, {
    pane: 'farmPrivatePane',
    style: (feature) => farmFieldStyle(feature?.properties?.field_id === selectedFieldId),
    onEachFeature: (feature, featureLayer) => {
      const props = feature.properties || {};
      lookup[props.field_id] = featureLayer;
      featureLayer.bindPopup(farmFieldPopup(props));
      createFarmAnchoredLabel({
        featureLayer,
        id: props.field_id,
        kind: 'field',
        title: props.unit_name || 'Campo',
        areaHa: props.area_ha,
        analytics: props.analytics || null,
      }).forEach((item) => labelLayer.addLayer(item));
      featureLayer.on('click', (event) => {
        if (stopEditorLayerClick(event)) return;
        highlightFarmField(props.field_id, true);
        if (onFieldSelect) onFieldSelect(props);
      });
    },
  }).addTo(store.map);
  labelLayer.addTo(store.map);
  setStore({
    farmFieldsLayer: layer,
    farmFieldsLookup: lookup,
    farmFieldLabelsLayer: labelLayer,
    selectedFieldId,
  });
  refreshFarmPrivateOverlays();
}

export function setFarmPaddocksOnMap(featureCollection, onPaddockSelect, selectedPaddockId = null) {
  if (!store.map) return;
  clearFarmPaddocksLayer();
  clearFarmPaddockLabelsLayer();
  const lookup = {};
  const labelLayer = window.L.layerGroup();
  const layer = window.L.geoJSON(featureCollection, {
    pane: 'farmPrivatePane',
    style: (feature) => farmPaddockStyle(feature?.properties?.paddock_id === selectedPaddockId),
    onEachFeature: (feature, featureLayer) => {
      const props = feature.properties || {};
      lookup[props.paddock_id] = featureLayer;
      featureLayer.bindPopup(paddockPopup(props));
      createFarmAnchoredLabel({
        featureLayer,
        id: props.paddock_id,
        kind: 'paddock',
        title: props.name || 'Potrero',
        areaHa: props.area_ha,
        analytics: props.analytics || null,
      }).forEach((item) => labelLayer.addLayer(item));
      featureLayer.on('click', (event) => {
        if (stopEditorLayerClick(event)) return;
        highlightFarmPaddock(props.paddock_id, true);
        if (onPaddockSelect) onPaddockSelect(props);
      });
    },
  }).addTo(store.map);
  labelLayer.addTo(store.map);
  setStore({
    farmPaddocksLayer: layer,
    farmPaddocksLookup: lookup,
    farmPaddockLabelsLayer: labelLayer,
    selectedPaddockId,
  });
  refreshFarmPrivateOverlays();
}

export function highlightFarmField(fieldId, fitBounds = false) {
  if (!store.farmFieldsLookup || !Object.keys(store.farmFieldsLookup).length) return;
  setStore({ selectedFieldId: fieldId });
  Object.entries(store.farmFieldsLookup).forEach(([id, layer]) => {
    layer.setStyle(farmFieldStyle(id === fieldId));
  });
  const layer = store.farmFieldsLookup[fieldId];
  if (!layer) return;
  if (layer.bringToFront) layer.bringToFront();
  fitLayerBounds(layer, fitBounds, 15);
  if (layer.openPopup) layer.openPopup();
}

export function highlightFarmPaddock(paddockId, fitBounds = false) {
  if (!store.farmPaddocksLookup || !Object.keys(store.farmPaddocksLookup).length) return;
  setStore({ selectedPaddockId: paddockId });
  Object.entries(store.farmPaddocksLookup).forEach(([id, layer]) => {
    layer.setStyle(farmPaddockStyle(id === paddockId));
  });
  const layer = store.farmPaddocksLookup[paddockId];
  if (!layer) return;
  if (layer.bringToFront) layer.bringToFront();
  fitLayerBounds(layer, fitBounds, 16);
  if (layer.openPopup) layer.openPopup();
}

export function startFarmGeometryEditor({ mode = 'field', geometry = null, onChange = null, onComplete = null } = {}) {
  if (!store.map || !window.L?.PM) return false;
  clearFarmGeometryEditor({ preserveDraft: true });
  disableFarmDrawModes();
  setFarmEditorMode(true);
  const keepFarmOverlaysVisible = mode === 'paddock';
  setFarmOverlayVisibility(keepFarmOverlaysVisible);
  syncOperationalOverlayInteractivity(false);
  syncFarmOverlayInteractivity(false);

  if (mode === 'paddock') {
    clearFarmEditorContextLayer();
  }

  const pathOptions = mode === 'paddock'
    ? { color: '#8cf0be', fillColor: '#2ecc71', fillOpacity: 0.08, weight: 2 }
    : { color: '#5db7ff', fillColor: '#4a90d9', fillOpacity: 0.12, weight: 3 };

  const handleDraftChange = (nextGeometry, layerRef) => {
    if (mode === 'paddock') {
      setStore({ paddockDraftGeometry: nextGeometry });
    } else {
      setStore({ fieldDraftGeometry: nextGeometry });
    }
    if (typeof onChange === 'function') onChange(nextGeometry, layerRef);
  };

  if (geometry) {
    const draftLayer = window.L.geoJSON({ type: 'Feature', geometry, properties: {} }, {
      pane: 'farmDraftPane',
      style: pathOptions,
    }).getLayers()[0];
    if (!draftLayer) return false;
    draftLayer.addTo(store.map);
    if (draftLayer.bringToFront) draftLayer.bringToFront();
      setStore({
        farmDraftLayer: draftLayer,
        farmDraftType: mode,
        farmEditorActive: true,
        farmDraftChangeHandler: handleDraftChange,
      });
      wireDraftLayer(draftLayer, handleDraftChange);
      fitLayerBounds(draftLayer, true, 16);
      if (typeof onComplete === 'function') {
        onComplete(geometry, draftLayer);
      }
      return true;
    }

  if (mode === 'paddock') {
      setStore({
        farmDraftLayer: null,
        farmDraftType: mode,
        farmEditorActive: true,
        farmDraftChangeHandler: handleDraftChange,
        farmDraftCreateHandler: null,
      });
      return startFarmManualPolygonCreate(mode, pathOptions, handleDraftChange, onComplete);
    }

  const handleCreate = (event) => {
    const createdLayer = recreateDraftLayer(event.layer, pathOptions);
    if (!createdLayer) {
      disableFarmDrawModes();
      return;
    }
    disableFarmDrawModes();
    if (store.farmDraftLayer && store.farmDraftLayer !== createdLayer) {
      store.map.removeLayer(store.farmDraftLayer);
    }
      setStore({
        farmDraftLayer: createdLayer,
        farmDraftType: mode,
        farmEditorActive: true,
        farmDraftChangeHandler: handleDraftChange,
        farmDraftCreateHandler: handleCreate,
      });
      wireDraftLayer(createdLayer, handleDraftChange);
      fitLayerBounds(createdLayer, true, 16);
      if (typeof onComplete === 'function') {
        onComplete(createdLayer.toGeoJSON()?.geometry || null, createdLayer);
      }
    };

  store.map.on('pm:create', handleCreate);
  store.map.pm.enableDraw('Polygon', {
    snappable: true,
    continueDrawing: false,
    allowSelfIntersection: false,
    pathOptions,
  });
  setStore({
    farmDraftLayer: null,
    farmDraftType: mode,
    farmEditorActive: true,
    farmDraftChangeHandler: handleDraftChange,
    farmDraftCreateHandler: handleCreate,
  });
  return true;
}

function syncConeatVisibilityHint() {
  if (!store.map || !isLayerActive('coneat')) return;
  if (store.map.getZoom() < CONEAT_MIN_VISIBLE_ZOOM) {
    showMapStatus(`CONEAT se visualiza desde zoom ${CONEAT_MIN_VISIBLE_ZOOM}. Acerque el mapa para ver los suelos.`, 3200);
  }
}

function ensureConeatVisibleZoom() {
  if (!store.map || store.map.getZoom() >= CONEAT_MIN_VISIBLE_ZOOM) return;

  const selectedDepartmentLayer = store.selectedDepartment
    ? store.departmentsLookup?.[store.selectedDepartment]
    : null;

  if (selectedDepartmentLayer?.getBounds) {
    store.map.fitBounds(selectedDepartmentLayer.getBounds(), { padding: [24, 24] });
    if (store.map.getZoom() < CONEAT_MIN_VISIBLE_ZOOM) {
      store.map.setZoom(CONEAT_MIN_VISIBLE_ZOOM);
    }
  } else {
    store.map.setZoom(CONEAT_MIN_VISIBLE_ZOOM);
  }

  showMapStatus(`CONEAT requiere mayor detalle. Acerqué el mapa a zoom ${CONEAT_MIN_VISIBLE_ZOOM}.`, 3400);
}

function departmentColor(props) {
  return sectionColor(props);
}

function departmentStyle(props, selected = false) {
  const opacity = layerOpacityValue('department', 0.85);
  const color = departmentColor(props);
  return {
    color: selected ? '#7dc7ff' : color,
    weight: selected ? 3 : 2,
    opacity: selected ? 1 : Math.max(0.75, opacity),
    fillColor: color,
    fillOpacity: selected ? Math.min(0.55, opacity * 0.6) : Math.min(0.4, opacity * 0.45),
  };
}

function sectionColor(props) {
  return props?.color
    || (props?.state === 'Emergencia' ? '#e74c3c'
      : props?.state === 'Alerta' ? '#e67e22'
        : props?.state === 'Vigilancia' ? '#f1c40f'
          : '#2ecc71');
}

function sectionStyle(props, selected = false) {
  const opacity = layerOpacityValue('judicial', 0.85);
  const color = sectionColor(props);
  return {
    color: selected ? '#4a90d9' : color,
    weight: selected ? 3 : 1.2,
    opacity: selected ? 1 : Math.max(0.55, opacity),
    fillColor: color,
    fillOpacity: selected ? Math.min(0.6, opacity * 0.7) : Math.min(0.42, opacity * 0.42),
  };
}

function hexStyle(props, selected = false) {
  const opacity = layerOpacityValue('hex', 0.85);
  const color = sectionColor(props);
  return {
    color: selected ? '#9ad8ff' : color,
    weight: selected ? 2.2 : 0.8,
    opacity: selected ? 1 : Math.max(0.45, opacity),
    fillColor: color,
    fillOpacity: selected ? Math.min(0.68, opacity * 0.75) : Math.min(0.5, opacity * 0.5),
  };
}

function productiveStyle(props, selected = false) {
  const opacity = layerOpacityValue('productiva', 0.85);
  const color = sectionColor(props);
  return {
    color: selected ? '#ffffff' : color,
    weight: selected ? 3.5 : 2,
    opacity: selected ? 1 : Math.max(0.7, opacity),
    fillColor: color,
    fillOpacity: selected ? Math.min(0.55, opacity * 0.6) : Math.min(0.28, opacity * 0.3),
  };
}

function sectionPopup(props) {
  const raw = props.raw_metrics || {};
  return `
    <div style="min-width:220px">
      <strong>${props.unit_name || 'Seccion'}</strong><br>
      <span style="color:#9fb0c7">${props.department || ''} · ${props.state || 'Sin dato'}</span><br>
      <div style="margin-top:6px">Risk: <strong>${props.risk_score ?? '—'}</strong> · Confianza: <strong>${props.confidence_score ?? '—'}</strong></div>
      <div>Humedad S1: <strong>${raw.s1_humidity_mean_pct ?? '—'}%</strong></div>
      <div>NDMI: <strong>${raw.s2_ndmi_mean ?? raw.estimated_ndmi ?? '—'}</strong></div>
      <div>SPI-30: <strong>${raw.spi_30d ?? '—'}</strong></div>
    </div>
  `;
}

function departmentPopup(props) {
  const raw = props.raw_metrics || {};
  return `
    <div style="min-width:230px">
      <strong>${props.unit_name || props.department || 'Departamento'}</strong><br>
      <span style="color:#9fb0c7">${props.state || 'Sin dato'} · ${props.cache_status || 'cache'}</span><br>
      <div style="margin-top:6px">Risk: <strong>${props.risk_score ?? '—'}</strong> · Confianza: <strong>${props.confidence_score ?? '—'}</strong></div>
      <div>Humedad S1: <strong>${raw.s1_humidity_mean_pct ?? '—'}%</strong></div>
      <div>NDMI: <strong>${raw.s2_ndmi_mean ?? raw.estimated_ndmi ?? '—'}</strong></div>
      <div>SPI-30: <strong>${raw.spi_30d ?? '—'}</strong></div>
      <div>Área afectada: <strong>${props.affected_pct ?? '—'}%</strong></div>
    </div>
  `;
}

function hexPopup(props) {
  const raw = props.raw_metrics || {};
  return `
    <div style="min-width:240px">
      <strong>${props.unit_name || 'Hexagono H3'}</strong><br>
      <span style="color:#9fb0c7">${props.department || ''} Â· ${props.state || 'Sin dato'} Â· r${props.h3_resolution ?? 'N/D'}</span><br>
      <div style="margin-top:6px">Risk: <strong>${props.risk_score ?? 'â€”'}</strong> Â· Confianza: <strong>${props.confidence_score ?? 'â€”'}</strong></div>
      <div>Humedad S1: <strong>${raw.s1_humidity_mean_pct ?? 'â€”'}%</strong></div>
      <div>NDMI: <strong>${raw.s2_ndmi_mean ?? raw.estimated_ndmi ?? 'â€”'}</strong></div>
      <div>SPI-30: <strong>${raw.spi_30d ?? 'â€”'}</strong></div>
      <div>Ãrea afectada: <strong>${props.affected_pct ?? 'â€”'}%</strong></div>
    </div>
  `;
}

function productivePopup(props) {
  const raw = props.raw_metrics || {};
  return `
    <div style="min-width:240px">
      <strong>${props.unit_name || 'Unidad productiva'}</strong><br>
      <span style="color:#9fb0c7">${props.department || ''} · ${props.unit_category || 'predio'} · ${props.state || 'Sin dato'}</span><br>
      <div style="margin-top:6px">Risk: <strong>${props.risk_score ?? '—'}</strong> · Confianza: <strong>${props.confidence_score ?? '—'}</strong></div>
      <div>Humedad S1: <strong>${raw.s1_humidity_mean_pct ?? '—'}%</strong></div>
      <div>NDMI: <strong>${raw.s2_ndmi_mean ?? raw.estimated_ndmi ?? '—'}</strong></div>
      <div>SPI-30: <strong>${raw.spi_30d ?? '—'}</strong></div>
    </div>
  `;
}

function applyDepartmentOpacity() {
  if (!store.departmentsLookup) return;
  Object.entries(store.departmentsLookup).forEach(([departmentName, layer]) => {
    layer.setStyle(departmentStyle(layer.feature?.properties, departmentName === store.selectedDepartment));
  });
}

function applySectionOpacity() {
  if (!store.sectionsLookup) return;
  Object.entries(store.sectionsLookup).forEach(([unitId, layer]) => {
    layer.setStyle(sectionStyle(layer.feature?.properties, unitId === store.selectedSectionId));
  });
}

function applyProductiveOpacity() {
  if (!store.productiveLookup) return;
  Object.entries(store.productiveLookup).forEach(([unitId, layer]) => {
    layer.setStyle(productiveStyle(layer.feature?.properties, unitId === store.selectedProductiveId));
  });
}

function applyHexOpacity() {
  if (!store.hexLookup) return;
  Object.entries(store.hexLookup).forEach(([unitId, layer]) => {
    layer.setStyle(hexStyle(layer.feature?.properties, unitId === store.selectedHexId));
  });
}

export async function initMap(onPolygonDraw, onDepartmentSelect, onSectionSelect) {
  // Confinamos el mapa a Uruguay: maxBounds + viscosidad 1.0 rebota si el usuario
  // intenta pan fuera; minZoom=6 evita zoom-out al globo entero. Este gate
  // reduce ~4-5x la cantidad de tiles pedidos por frame (antes incluía pampa
  // argentina, sur de Brasil y océano Atlántico sin datos útiles).
  const map = window.L.map('map', {
    zoomControl: true,
    doubleClickZoom: false,
    maxBounds: URUGUAY_BOUNDS,
    maxBoundsViscosity: 1.0,
    minZoom: URUGUAY_MIN_ZOOM,
    maxZoom: URUGUAY_MAX_ZOOM,
  }).setView(INITIAL_VIEW.center, INITIAL_VIEW.zoom);
  // Pane de la máscara de clipping (entre OSM=200 y satellitePane=380)
  map.createPane('clipMaskPane');
  map.getPane('clipMaskPane').style.zIndex = 375;
  map.getPane('clipMaskPane').style.pointerEvents = 'none';
  map.createPane('satellitePane');
  map.getPane('satellitePane').style.zIndex = 380;
  map.createPane('officialOverlayPane');
  map.getPane('officialOverlayPane').style.zIndex = 405;
  map.createPane('farmGuidePane');
  map.getPane('farmGuidePane').style.zIndex = 610;
  map.createPane('farmPrivatePane');
  map.getPane('farmPrivatePane').style.zIndex = 620;
  map.createPane('farmTetherPane');
  map.getPane('farmTetherPane').style.zIndex = 625;
  map.createPane('farmLabelPane');
  map.getPane('farmLabelPane').style.zIndex = 626;
  map.createPane('farmDraftPane');
  map.getPane('farmDraftPane').style.zIndex = 630;
  const baseTileLayer = window.L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '© OpenStreetMap',
    opacity: 0.75,
    bounds: URUGUAY_BOUNDS,
    noWrap: true,
  }).addTo(map);

  if (map.pm) {
    map.pm.setGlobalOptions({
      continueDrawing: false,
      allowSelfIntersection: false,
      snappable: true,
    });
  }

  const nextOpacities = { ...(store.layerOpacities || {}) };
  BUILTIN_LAYER_DEFS.forEach((definition) => {
    if (nextOpacities[definition.id] == null) nextOpacities[definition.id] = Number(definition.opacityDefault || 0.85);
  });
  const nextActiveLayers = Array.isArray(store.activeLayers) && store.activeLayers.length
    ? orderedActiveLayerIds(store.activeLayers)
    : ['alerta'];
  setStore({
    map,
    onPolygonDraw,
    onSectionSelect,
    baseTileLayer,
    activeLayers: nextActiveLayers,
    currentLayer: store.currentLayer || nextActiveLayers[nextActiveLayers.length - 1] || 'alerta',
    layerOpacities: nextOpacities,
    timelineDate: store.timelineDate || todayIsoDate(),
    timelineWindowDays: TIMELINE_WINDOW_DAYS,
    timelineSpeed: normalizeTimelineSpeed(store.timelineSpeed),
  });
  exposeMapControls(onDepartmentSelect);
  ensureLayerControlEvents();
  ensureTimelineControlEvents();
  renderLayerMenu();
  renderTimelineControls();
  renderActiveTileLayers();
  map.on('moveend zoomend', () => scheduleTimelineManifestRefresh({ preserveDate: true }));
  window.setTimeout(() => {
    refreshTimelineManifest({ preserveDate: false }).catch((error) => {
      console.warn('No se pudo inicializar la timeline base del mapa:', error);
    });
  }, 0);
  return map;
}

function exposeMapControls(onDepartmentSelect) {
  window._drawingMode = false;
  window._drawingPoints = [];
  window._previewPoly = null;
  window._previewMarkers = [];
  window._savedPoly = null;

  window.startDrawing = () => {
    window._drawingMode = true;
    window._drawingPoints = [];
    document.getElementById('btn-limpiar').style.display = 'inline-flex';
    document.getElementById('scope-badge-value').textContent = 'Dibujando parcela';
    store.map.getContainer().style.cursor = 'crosshair';
  };

  window.finishDrawing = () => {
    if (window._drawingPoints.length < 3) return;
    const coords = window._drawingPoints.map((point) => [point.lng, point.lat]);
    coords.push(coords[0]);
    const geojson = { type: 'Polygon', coordinates: [coords] };
    if (window._savedPoly) store.map.removeLayer(window._savedPoly);
    window._savedPoly = window.L.polygon(window._drawingPoints.map((point) => [point.lat, point.lng]), {
      color: '#2ecc71',
      weight: 2,
      fillColor: '#4a90d9',
      fillOpacity: 0.2,
    }).addTo(store.map);
    window._drawingMode = false;
    store.map.getContainer().style.cursor = '';
    if (store.onPolygonDraw) store.onPolygonDraw(geojson);
  };

  window.clearDrawing = () => {
    window._drawingMode = false;
    window._drawingPoints = [];
    document.getElementById('btn-limpiar').style.display = 'none';
    document.getElementById('scope-badge-value').textContent = 'Uruguay';
    if (window._savedPoly) store.map.removeLayer(window._savedPoly);
    window._savedPoly = null;
    window._previewMarkers.forEach((marker) => store.map.removeLayer(marker));
    window._previewMarkers = [];
    if (window._previewPoly) store.map.removeLayer(window._previewPoly);
    window._previewPoly = null;
  };

  window.setLayer = (name, btn) => setLayer(name, btn);
  window.toggleMapLayer = (name, active) => toggleMapLayer(name, active);
  window.setTileOpacity = (value) => setTileOpacity(value);
  window.restoreMapInitialView = () => restoreMapInitialView();
  window.applyRecommendedMapLayers = () => applyRecommendedLayers();
  window.clearMapLayers = () => clearAllMapLayers();

  store.map.on('click', (event) => {
    if (!window._drawingMode) return;
    window._drawingPoints.push(event.latlng);
    const marker = window.L.circleMarker(event.latlng, { radius: 4, color: '#4a90d9', weight: 1 }).addTo(store.map);
    window._previewMarkers.push(marker);
    if (window._previewPoly) store.map.removeLayer(window._previewPoly);
    if (window._drawingPoints.length > 1) {
      window._previewPoly = window.L.polygon(window._drawingPoints.map((point) => [point.lat, point.lng]), {
        color: '#4a90d9',
        weight: 2,
        fillOpacity: 0.08,
        dashArray: '6 4',
      }).addTo(store.map);
    }
  });

  store.map.on('dblclick', () => {
    if (window._drawingMode) window.finishDrawing();
  });

  store.map.on('zoomend', () => {
    if (store.focusMarker && store.focusMarker.openTooltip) store.focusMarker.openTooltip();
    syncConeatVisibilityHint();
    updateFarmLabelScales();
  });

  store.map.on('load', () => {
    if (onDepartmentSelect) onDepartmentSelect();
  });
}

export function setUnitsOnMap(units, onDepartmentSelect) {
  if (!store.map) return;
  clearMarkers();
  const markers = units.map((unit) => {
    const color = unit.state === 'Emergencia' ? '#e74c3c' : unit.state === 'Alerta' ? '#e67e22' : unit.state === 'Vigilancia' ? '#f1c40f' : '#2ecc71';
    const marker = window.L.circleMarker([unit.centroid_lat, unit.centroid_lon], {
      radius: 8,
      color,
      fillColor: color,
      fillOpacity: 0.7,
      weight: 2,
    })
      .bindTooltip(`${unit.department} · ${unit.state || 'Sin dato'} · riesgo ${unit.risk_score ?? '—'}`)
      .on('click', () => onDepartmentSelect(unit.department))
      .addTo(store.map);
    return marker;
  });
  setStore({ unitMarkers: markers });
}

/**
 * True cuando el usuario está trabajando sobre una farm: establecimiento,
 * field, paddock o búsqueda de padrón activa. En ese estado NO queremos
 * que clicks sobre departamentos / secciones / hexágonos / predios hagan
 * fitBounds — eso saca al usuario del zoom al padrón.
 */
function isFarmContextActive() {
  return Boolean(
    store.selectedEstablishmentId
    || store.selectedFieldId
    || store.selectedPaddockId
    || store.selectedPadronSearch
  );
}

export function setDepartmentsOnMap(featureCollection, onDepartmentSelect, selectedDepartment = null) {
  if (!store.map) return;
  clearDepartmentLayer();
  const cacheStatus = featureCollection?.metadata?.cache_status || null;
  const departmentsLookup = {};
  const layer = window.L.geoJSON(featureCollection, {
    style: (feature) => departmentStyle(feature.properties, feature.properties.department === selectedDepartment),
    onEachFeature: (feature, featureLayer) => {
      const props = { ...(feature.properties || {}), cache_status: (feature.properties || {}).cache_status || cacheStatus };
      departmentsLookup[props.department] = featureLayer;
      featureLayer.bindPopup(departmentPopup(props));
      featureLayer.on('click', (event) => {
        if (stopEditorLayerClick(event)) return;
        if (isFarmContextActive()) return;  // no sacar al usuario del zoom al padrón
        highlightDepartment(props.department, true);
        if (onDepartmentSelect) onDepartmentSelect(props.department);
      });
    },
  }).addTo(store.map);

  setStore({ departmentsLayer: layer, departmentsLookup, selectedDepartment });
}

export function setSectionsOnMap(featureCollection, onSectionSelect, selectedSectionId = null) {
  if (!store.map) return;
  clearSectionsLayer();
  const sectionsLookup = {};
  const layer = window.L.geoJSON(featureCollection, {
    style: (feature) => sectionStyle(feature.properties, feature.properties.unit_id === selectedSectionId),
    onEachFeature: (feature, featureLayer) => {
      const props = feature.properties || {};
      sectionsLookup[props.unit_id] = featureLayer;
      featureLayer.bindPopup(sectionPopup(props));
      featureLayer.on('click', (event) => {
        if (stopEditorLayerClick(event)) return;
        if (isFarmContextActive()) return;
        highlightSection(props.unit_id, true);
        if (onSectionSelect) onSectionSelect(props);
      });
    },
  }).addTo(store.map);

  setStore({ sectionsLayer: layer, sectionsLookup, selectedSectionId });
}

export function setProductivesOnMap(featureCollection, onProductiveSelect, selectedProductiveId = null) {
  if (!store.map) return;
  clearProductiveLayer();
  const productiveLookup = {};
  const layer = window.L.geoJSON(featureCollection, {
    style: (feature) => productiveStyle(feature.properties, feature.properties.unit_id === selectedProductiveId),
    onEachFeature: (feature, featureLayer) => {
      const props = feature.properties || {};
      productiveLookup[props.unit_id] = featureLayer;
      featureLayer.bindPopup(productivePopup(props));
      featureLayer.on('click', (event) => {
        if (stopEditorLayerClick(event)) return;
        if (isFarmContextActive()) return;
        highlightProductive(props.unit_id, true);
        if (onProductiveSelect) onProductiveSelect(props);
      });
    },
  }).addTo(store.map);

  setStore({ productiveLayer: layer, productiveLookup, selectedProductiveId });
}

export function setHexesOnMap(featureCollection, onHexSelect, selectedHexId = null) {
  if (!store.map) return;
  clearHexLayer();
  const hexLookup = {};
  const layer = window.L.geoJSON(featureCollection, {
    style: (feature) => hexStyle(feature.properties, feature.properties.unit_id === selectedHexId),
    onEachFeature: (feature, featureLayer) => {
      const props = feature.properties || {};
      hexLookup[props.unit_id] = featureLayer;
      featureLayer.bindPopup(hexPopup(props));
      featureLayer.on('click', (event) => {
        if (stopEditorLayerClick(event)) return;
        if (isFarmContextActive()) return;
        highlightHex(props.unit_id, true);
        if (onHexSelect) onHexSelect(props);
      });
    },
  }).addTo(store.map);

  setStore({ hexLayer: layer, hexLookup, selectedHexId });
}

export function highlightDepartment(departmentName, fitBounds = false) {
  if (!store.departmentsLookup || !Object.keys(store.departmentsLookup).length) return;
  setStore({ selectedDepartment: departmentName });
  applyDepartmentOpacity();
  const layer = store.departmentsLookup[departmentName];
  if (!layer) return;
  if (layer.bringToFront) layer.bringToFront();
  if (fitBounds && layer.getBounds) {
    store.map.fitBounds(layer.getBounds(), { padding: [20, 20], maxZoom: 8 });
  }
  if (layer.openPopup) layer.openPopup();
}

export function highlightSection(sectionId, fitBounds = false) {
  if (!store.sectionsLookup || !Object.keys(store.sectionsLookup).length) return;
  setStore({ selectedSectionId: sectionId });
  applySectionOpacity();
  const layer = store.sectionsLookup[sectionId];
  if (!layer) return;
  if (layer.bringToFront) layer.bringToFront();
  if (fitBounds && layer.getBounds) {
    store.map.fitBounds(layer.getBounds(), { padding: [20, 20], maxZoom: 10 });
  }
  if (layer.openPopup) layer.openPopup();
}

export function highlightProductive(unitId, fitBounds = false) {
  if (!store.productiveLookup || !Object.keys(store.productiveLookup).length) return;
  setStore({ selectedProductiveId: unitId });
  applyProductiveOpacity();
  const layer = store.productiveLookup[unitId];
  if (!layer) return;
  if (layer.bringToFront) layer.bringToFront();
  if (fitBounds && layer.getBounds) {
    store.map.fitBounds(layer.getBounds(), { padding: [20, 20], maxZoom: 13 });
  }
  if (layer.openPopup) layer.openPopup();
}

export function highlightHex(hexId, fitBounds = false) {
  if (!store.hexLookup || !Object.keys(store.hexLookup).length) return;
  setStore({ selectedHexId: hexId });
  applyHexOpacity();
  const layer = store.hexLookup[hexId];
  if (!layer) return;
  if (layer.bringToFront) layer.bringToFront();
  if (fitBounds && layer.getBounds) {
    store.map.fitBounds(layer.getBounds(), { padding: [20, 20], maxZoom: 10 });
  }
  if (layer.openPopup) layer.openPopup();
}

export function updateFocus(model) {
  if (!store.map) return;
  // Preserve viewport whenever the user is working on a farm (establecimiento, field or paddock).
  // A fresh establishment may have no field/paddock yet but the padrón was already zoomed in by
  // setFarmGuideOnMap — we must not undo that zoom from a downstream loadSelection call.
  const preserveFarmViewport = Boolean(
    store.selectedEstablishmentId || store.selectedFieldId || store.selectedPaddockId || store.selectedPadronSearch
  );
  if (store.focusMarker) store.map.removeLayer(store.focusMarker);
  if (model.unitLat && model.unitLon) {
    store.focusMarker = window.L.marker([model.unitLat, model.unitLon]).addTo(store.map);
    store.focusMarker.bindPopup(`<strong>${model.scopeLabel}</strong><br>${model.title}<br>Risk ${model.riskScore ?? '—'}`);
    if (!preserveFarmViewport && !((isLayerActive('judicial') && store.selectedSectionId) || (isLayerActive('productiva') && store.selectedProductiveId) || (isLayerActive('hex') && store.selectedHexId))) {
      store.map.setView([model.unitLat, model.unitLon], model.scope === 'nacional' ? 7 : 9);
    }
  } else if (!preserveFarmViewport) {
    store.map.setView(INITIAL_VIEW.center, INITIAL_VIEW.zoom);
  }
}

function applyLayerOpacitySideEffects(layerId) {
  if (layerId === 'judicial') {
    applySectionOpacity();
    return;
  }
  if (layerId === 'productiva') {
    applyProductiveOpacity();
    return;
  }
  if (layerId === 'hex') {
    applyHexOpacity();
    return;
  }
  if (layerId === 'department') {
    applyDepartmentOpacity();
  }
}

async function notifyLayerStateChange(layerId, active) {
  if (typeof store.onMapLayerChange !== 'function') return;
  const definition = getLayerDefinition(layerId);
  try {
    await Promise.resolve(store.onMapLayerChange({ layerId, active, definition }));
  } catch (error) {
    console.warn(`No se pudo sincronizar la capa ${layerId}:`, error);
  }
}

export async function toggleMapLayer(layerId, desiredActive = null) {
  if (!store.map) return;
  const definition = getLayerDefinition(layerId);
  if (!definition) return;
  const activeNow = isLayerActive(layerId);
  const nextActive = desiredActive == null ? !activeNow : Boolean(desiredActive);
  const nextLayers = nextActive
    ? orderedActiveLayerIds([...new Set([...(store.activeLayers || []), layerId])])
    : orderedActiveLayerIds((store.activeLayers || []).filter((id) => id !== layerId));
  const nextOpacities = { ...(store.layerOpacities || {}) };
  if (nextOpacities[layerId] == null) {
    nextOpacities[layerId] = Number(definition.opacityDefault || 0.85);
  }
  setStore({
    activeLayers: nextLayers,
    layerOpacities: nextOpacities,
    currentLayer: nextLayers[nextLayers.length - 1] || layerId,
  });
  renderActiveTileLayers();
  applyLayerOpacitySideEffects(layerId);
  renderLayerMenu();
  if (layerId === 'coneat') syncConeatVisibilityHint();
  refreshFarmPrivateOverlays();
  await notifyLayerStateChange(layerId, nextActive);
  scheduleTimelineManifestRefresh({ preserveDate: true });
}

export function setLayerOpacityValue(layerId, value) {
  const clamped = Math.max(0.15, Math.min(1, Number(value) || 0.85));
  const nextOpacities = { ...(store.layerOpacities || {}), [layerId]: clamped };
  setStore({ layerOpacities: nextOpacities });
  const layerInstance = store.layerInstances?.[layerId];
  if (layerInstance?.setOpacity) layerInstance.setOpacity(clamped);
  applyLayerOpacitySideEffects(layerId);
  renderLayerMenu();
}

export async function clearAllMapLayers() {
  const activeIds = [...(store.activeLayers || [])];
  for (const layerId of activeIds) {
    await toggleMapLayer(layerId, false);
  }
  setStore({ currentLayer: 'alerta' });
  renderLayerMenu();
  scheduleTimelineManifestRefresh({ preserveDate: false });
}

export async function applyRecommendedLayers() {
  const targetIds = new Set(RECOMMENDED_LAYER_IDS.filter((layerId) => getLayerDefinition(layerId)));
  const currentIds = new Set(store.activeLayers || []);
  for (const layerId of currentIds) {
    if (!targetIds.has(layerId)) await toggleMapLayer(layerId, false);
  }
  for (const layerId of targetIds) {
    if (!currentIds.has(layerId)) await toggleMapLayer(layerId, true);
  }
  renderLayerMenu();
  scheduleTimelineManifestRefresh({ preserveDate: true });
}

export function restoreMapInitialView() {
  if (!store.map) return;
  store.map.setView(INITIAL_VIEW.center, INITIAL_VIEW.zoom, { animate: false });
  scheduleTimelineManifestRefresh({ preserveDate: true });
}

export function setAvailableOverlays(items = []) {
  const normalized = (items || []).map((item) => ({
    id: item.id,
    label: item.label,
    category: item.category,
    provider: item.provider,
    type: 'official',
    serviceKind: item.service_kind,
    serviceUrl: item.service_url,
    layers: item.layers,
    minZoom: Number(item.min_zoom || 0),
    opacityDefault: Number(item.opacity_default || 0.85),
    zIndexPriority: Number(item.z_index_priority || 300),
    attribution: item.attribution,
    cacheNamespace: item.cache_namespace,
    recommended: Boolean(item.recommended),
  }));
  const nextOpacities = { ...(store.layerOpacities || {}) };
  normalized.forEach((definition) => {
    if (nextOpacities[definition.id] == null) nextOpacities[definition.id] = definition.opacityDefault;
  });
  BUILTIN_LAYER_DEFS.forEach((definition) => {
    if (nextOpacities[definition.id] == null) nextOpacities[definition.id] = Number(definition.opacityDefault || 0.85);
  });
  setStore({ availableOverlays: normalized, layerOpacities: nextOpacities });
  renderLayerMenu();
  renderActiveTileLayers();
}

export function setMapLayerChangeHandler(handler) {
  setStore({ onMapLayerChange: handler || null });
}

export async function setLayer(name) {
  await toggleMapLayer(name, true);
}

export function setTileOpacity(value) {
  const targetLayerId = store.currentLayer || (store.activeLayers || []).slice(-1)[0];
  if (!targetLayerId) return;
  setLayerOpacityValue(targetLayerId, value);
}
