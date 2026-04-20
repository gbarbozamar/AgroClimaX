/**
 * Clipping visual del mapa Leaflet.
 *
 * Construye un polígono "donut" = mundo entero (ring exterior clockwise)
 * + scope (ring interior counter-clockwise, como hole). Cuando lo renderizamos
 * con fill negro 55% alpha, queda todo fuera del scope oscurecido sin tapar
 * la zona activa.
 */
import { store, setStore } from './state.js?v=20260420-4';
import { diagnostics } from './diagnostics.js?v=20260420-4';

const CLIP_MASK_PANE = 'clipMaskPane';
const WORLD_RING = [[-85, -180], [-85, 180], [85, 180], [85, -180], [-85, -180]];

/** Garantiza que el pane existe (z-index 375, entre OSM y analytic tiles). */
export function ensureClipMaskPane(map) {
  if (!map.getPane(CLIP_MASK_PANE)) {
    map.createPane(CLIP_MASK_PANE);
    map.getPane(CLIP_MASK_PANE).style.zIndex = 375;
    map.getPane(CLIP_MASK_PANE).style.pointerEvents = 'none';
  }
}

/** Fetch con cache del polígono del scope. */
export async function fetchScopeGeometry(scope, ref) {
  if (!scope || scope === 'nacional') {
    const cached = store.scopeGeometryCache?.get?.('nacional:null');
    if (cached) return cached;
    const resp = await fetch('/api/v1/geojson/uruguay');
    if (!resp.ok) throw new Error(`Failed to fetch uruguay geojson: ${resp.status}`);
    const data = await resp.json();
    if (!store.scopeGeometryCache) setStore({ scopeGeometryCache: new Map() });
    store.scopeGeometryCache.set('nacional:null', data);
    return data;
  }
  const cacheKey = `${scope}:${ref || ''}`;
  const cached = store.scopeGeometryCache?.get?.(cacheKey);
  if (cached) return cached;
  const resp = await fetch(`/api/v1/geojson/${encodeURIComponent(scope)}/${encodeURIComponent(ref)}`);
  if (!resp.ok) {
    diagnostics.log('warn', `No se pudo cargar geometría ${scope}/${ref}: HTTP ${resp.status}`);
    return null;
  }
  const data = await resp.json();
  if (!store.scopeGeometryCache) setStore({ scopeGeometryCache: new Map() });
  store.scopeGeometryCache.set(cacheKey, data);
  return data;
}

/** Extrae coordenadas del primer polygon/multipolygon del FeatureCollection. */
function extractPolygonRings(featureCollection) {
  const feature = featureCollection?.features?.[0];
  if (!feature) return [];
  const { type, coordinates } = feature.geometry || {};
  if (type === 'Polygon') return [coordinates[0]];  // solo outer ring del polygon
  if (type === 'MultiPolygon') return coordinates.map((poly) => poly[0]);
  return [];
}

/** Convierte ring [lng, lat] -> [lat, lng] que es lo que Leaflet espera. */
function ringToLatLng(ring) {
  return ring.map(([lng, lat]) => [lat, lng]);
}

/**
 * Construye GeoJSON del donut: mundo como outer ring + scope rings como holes.
 * Leaflet sí soporta holes en Polygon via array de múltiples rings.
 */
function buildDonutGeoJSON(scopeFc) {
  const scopeRings = extractPolygonRings(scopeFc).map(ringToLatLng);
  if (!scopeRings.length) return null;
  // Leaflet L.polygon recibe arrays de arrays [latLng]. El primer array es outer,
  // los siguientes son holes.
  return {
    outer: WORLD_RING,
    holes: scopeRings,
  };
}

/** Borra la máscara existente. */
function clearExistingMask() {
  if (store.clipMaskLayer) {
    try { store.map?.removeLayer(store.clipMaskLayer); } catch (_) { /* noop */ }
    setStore({ clipMaskLayer: null });
  }
}

/** Aplica la máscara al mapa para el scope actual. */
export async function applyClipMask(map, scope, ref) {
  if (!map) return;
  ensureClipMaskPane(map);
  try {
    const scopeFc = await fetchScopeGeometry(scope, ref);
    if (!scopeFc) { clearExistingMask(); return; }
    const donut = buildDonutGeoJSON(scopeFc);
    if (!donut) { clearExistingMask(); return; }
    clearExistingMask();
    // L.polygon con holes: primer elemento = outer, resto = inner rings
    const layer = window.L.polygon([donut.outer, ...donut.holes], {
      pane: CLIP_MASK_PANE,
      fillColor: '#02060d',
      fillOpacity: 0.65,
      stroke: false,
      interactive: false,
      smoothFactor: 1.0,
    }).addTo(map);
    setStore({ clipMaskLayer: layer });
    diagnostics.log('info', `clipMask aplicada scope=${scope} ref=${ref || ''}`);
  } catch (err) {
    diagnostics.log('error', `applyClipMask falló: ${err.message}`);
  }
}

/** Limpia la máscara (útil al entrar a modo "sin clip"). */
export function removeClipMask() {
  clearExistingMask();
}
