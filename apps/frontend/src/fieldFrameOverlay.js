/**
 * @deprecated fieldFrameOverlay reemplazado por fieldFrameLightbox.
 *
 * La funcionalidad de overlay sobre el mapa Leaflet fue eliminada — ahora
 * al clickear un frame del slider abre un modal lightbox con la imagen
 * agrandada + metadata + navegación. Ver fieldFrameLightbox.js.
 *
 * Este módulo queda como no-op para no romper imports externos; exporta
 * las funciones pero no hacen nada.
 */

export function showFieldFrameOverlay() {
  // no-op: reemplazado por fieldFrameLightbox
}

export function hideFieldFrameOverlay() {
  // no-op: reemplazado por fieldFrameLightbox
}

export function hasFieldFrameOverlay() {
  return false;
}
