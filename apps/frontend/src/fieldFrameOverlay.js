/**
 * Fase 3/4 B4 — Overlay del snapshot del campo sobre el mapa Leaflet.
 *
 * Cuando el usuario clickea un frame del fieldFrameSlider, mostramos el
 * PNG como imageOverlay en el mapa, clipeado al bbox del snapshot. Se
 * usa una sola layer reutilizable (se reemplaza al click del próximo
 * frame, se remueve al salir del scope field).
 */
import { store } from './state.js?v=20260421-1';

let _overlayLayer = null;

/**
 * Pinta un snapshot PNG como overlay sobre el mapa Leaflet.
 * @param {string} imageUrl  URL del PNG (relativa a /api/v1 o absoluta).
 * @param {[number,number,number,number]} bbox  [W, S, E, N] en lat/lng.
 */
export function showFieldFrameOverlay(imageUrl, bbox) {
  if (!window.L || !store.map) return;
  if (!imageUrl || !Array.isArray(bbox) || bbox.length !== 4) {
    hideFieldFrameOverlay();
    return;
  }
  const [w, s, e, n] = bbox;
  if (![w, s, e, n].every(Number.isFinite)) {
    hideFieldFrameOverlay();
    return;
  }
  hideFieldFrameOverlay();
  try {
    const bounds = [[s, w], [n, e]]; // Leaflet: [[south, west], [north, east]]
    _overlayLayer = window.L.imageOverlay(imageUrl, bounds, {
      opacity: 0.75,
      interactive: false,
      className: 'field-frame-image-overlay',
      zIndex: 450, // por encima de tiles pero debajo de controles
    });
    _overlayLayer.addTo(store.map);
  } catch (err) {
    _overlayLayer = null;
    // Log silencioso — no queremos ruido en console por errores de Leaflet.
  }
}

export function hideFieldFrameOverlay() {
  if (_overlayLayer && store.map) {
    try { store.map.removeLayer(_overlayLayer); } catch (_) { /* noop */ }
  }
  _overlayLayer = null;
}

export function hasFieldFrameOverlay() {
  return Boolean(_overlayLayer);
}
