/**
 * Fase 3 — Timeline propio del campo.
 *
 * Cuando el usuario entra en scope 'field', cambiamos la fuente del slider
 * del timeline global al listado de FieldImageSnapshot persistidos del
 * campo. Al salir volvemos al timeline global.
 *
 * El endpoint /api/v1/campos/{id}/timeline-frames devuelve:
 *   { field_id, layer_key, total, days: [{observed_at, image_url, metadata}] }
 */
import { store, setStore } from './state.js?v=20260421-1';
import { diagnostics } from './diagnostics.js?v=20260421-1';

const API_V1 = '/api/v1';

export async function loadFieldTimelineFrames(fieldId, layerKey = 'ndvi', days = 30) {
  if (!fieldId) {
    clearFieldTimeline();
    return;
  }
  try {
    const resp = await fetch(
      `${API_V1}/campos/${encodeURIComponent(fieldId)}/timeline-frames?layer=${encodeURIComponent(layerKey)}&days=${days}`,
      { credentials: 'same-origin' },
    );
    if (!resp.ok) {
      diagnostics.log('warn', `fieldTimeline: HTTP ${resp.status} para field=${fieldId} layer=${layerKey}`);
      setStore({ fieldTimelineSource: 'global', fieldTimelineFrames: [], fieldTimelineDate: null });
      return;
    }
    const data = await resp.json();
    const frames = Array.isArray(data?.days) ? data.days : [];
    setStore({
      fieldTimelineFrames: frames,
      fieldTimelineSource: 'field',
      fieldTimelineDate: frames[frames.length - 1]?.observed_at || null,
    });
    diagnostics.log('info', `fieldTimeline: ${frames.length} frames cargados field=${fieldId} layer=${layerKey}`);
  } catch (err) {
    diagnostics.log('warn', `fieldTimeline fetch err: ${err.message}`);
    setStore({ fieldTimelineSource: 'global', fieldTimelineFrames: [], fieldTimelineDate: null });
  }
}

export function clearFieldTimeline() {
  setStore({
    fieldTimelineFrames: [],
    fieldTimelineSource: 'global',
    fieldTimelineDate: null,
  });
}

// Suscripción al evento custom 'agroclimax:scope-change' para auto-cargar
// o limpiar el timeline del campo cuando cambia el scope.
if (typeof window !== 'undefined') {
  window.addEventListener('agroclimax:scope-change', (evt) => {
    const detail = evt?.detail || {};
    if (detail.scope === 'field' && detail.ref) {
      loadFieldTimelineFrames(detail.ref, 'ndvi', 30);
    } else {
      clearFieldTimeline();
    }
  });
}
