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

async function _fetchLayersAvailable(fieldId) {
  try {
    const resp = await fetch(
      `${API_V1}/campos/${encodeURIComponent(fieldId)}/layers-available`,
      { credentials: 'same-origin' },
    );
    if (!resp.ok) {
      diagnostics.log('warn', `fieldTimeline layers-available HTTP ${resp.status} field=${fieldId}`);
      return [];
    }
    const data = await resp.json();
    return Array.isArray(data?.layers) ? data.layers : [];
  } catch (err) {
    diagnostics.log('warn', `fieldTimeline layers-available err: ${err.message}`);
    return [];
  }
}

export async function loadFieldTimelineFrames(fieldId, layerKey = 'ndvi', days = 30) {
  if (!fieldId) {
    clearFieldTimeline();
    return;
  }
  try {
    const [resp, availableLayers] = await Promise.all([
      fetch(
        `${API_V1}/campos/${encodeURIComponent(fieldId)}/timeline-frames?layer=${encodeURIComponent(layerKey)}&days=${days}`,
        { credentials: 'same-origin' },
      ),
      _fetchLayersAvailable(fieldId),
    ]);
    if (!resp.ok) {
      diagnostics.log('warn', `fieldTimeline: HTTP ${resp.status} para field=${fieldId} layer=${layerKey}`);
      setStore({ fieldTimelineSource: 'global', fieldTimelineFrames: [], fieldTimelineDate: null });
      _unmountFieldSlider();
      return;
    }
    const data = await resp.json();
    const frames = Array.isArray(data?.days) ? data.days : [];
    const selectedDate = frames[frames.length - 1]?.observed_at || null;
    setStore({
      fieldTimelineFrames: frames,
      fieldTimelineSource: 'field',
      fieldTimelineDate: selectedDate,
    });
    diagnostics.log('info', `fieldTimeline: ${frames.length} frames cargados field=${fieldId} layer=${layerKey}`);
    _mountFieldSlider(frames, fieldId, layerKey, selectedDate, availableLayers, days);
  } catch (err) {
    diagnostics.log('warn', `fieldTimeline fetch err: ${err.message}`);
    setStore({ fieldTimelineSource: 'global', fieldTimelineFrames: [], fieldTimelineDate: null });
    _unmountFieldSlider();
  }
}

export function clearFieldTimeline() {
  setStore({
    fieldTimelineFrames: [],
    fieldTimelineSource: 'global',
    fieldTimelineDate: null,
  });
  _unmountFieldSlider();
  // El lightbox se auto-cierra (Escape / click-outside / close-button); no requiere cleanup externo.
}

async function _mountFieldSlider(frames, fieldId, layerKey, selectedDate, availableLayers = [], days = 30) {
  if (!frames?.length) {
    _unmountFieldSlider();
    return;
  }
  const container = document.getElementById('field-frame-slider-mount') || _ensureSliderMount();
  try {
    const mod = await import('./fieldFrameSlider.js?v=20260421-2');
    mod.injectFieldFrameSliderStyles?.();
    mod.renderFieldFrameSlider(container, frames, {
      layerKey,
      selectedDate,
      availableLayers,
      fieldName: store.selectedFieldDetail?.name || 'campo',
      onLayerChange: async (newLayer) => {
        if (!newLayer || newLayer === layerKey) return;
        diagnostics.log('info', `fieldTimeline: cambio de layer ${layerKey} -> ${newLayer}`);
        await loadFieldTimelineFrames(fieldId, newLayer, days);
      },
      onSelect: async (frame) => {
        setStore({ fieldTimelineDate: frame.observed_at });
        diagnostics.log('info', `fieldTimeline: frame seleccionado ${frame.observed_at}`);
        // A2: abrir lightbox modal en lugar de overlay sobre el mapa.
        try {
          const lb = await import('./fieldFrameLightbox.js?v=20260422-1');
          const idx = Math.max(0, frames.findIndex(f => f?.observed_at === frame.observed_at));
          lb.openFieldFrameLightbox?.(frames, idx, {
            fieldName: store.selectedFieldDetail?.name || 'Campo',
            layerKey,
          });
        } catch (err) {
          diagnostics.log('warn', `fieldFrameLightbox no disponible: ${err.message}`);
        }
      },
    });
  } catch (err) {
    diagnostics.log('warn', `fieldFrameSlider no disponible: ${err.message}`);
  }
}

function _ensureSliderMount() {
  let el = document.getElementById('field-frame-slider-mount');
  if (el) return el;
  el = document.createElement('div');
  el.id = 'field-frame-slider-mount';
  el.className = 'field-frame-slider-mount';
  const dock = document.querySelector('.map-timeline') || document.querySelector('.timeline-dock');
  if (dock?.parentElement) dock.parentElement.insertBefore(el, dock);
  else document.body.appendChild(el);
  return el;
}

function _unmountFieldSlider() {
  const el = document.getElementById('field-frame-slider-mount');
  if (el) el.innerHTML = '';
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
