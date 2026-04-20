/**
 * Coordinador de "scope" (nacional / departamento / seccion / field).
 *
 * Único punto que:
 *   - Guarda store.clipScope / clipRef
 *   - Aplica la máscara visual
 *   - Redibuja las capas analíticas con los nuevos query params
 *   - Trackea en diagnostics
 */
import { store, setStore } from './state.js?v=20260420-4';
import { diagnostics } from './diagnostics.js?v=20260420-4';
import { applyClipMask } from './clipMask.js?v=20260420-4';

const VALID_SCOPES = new Set(['nacional', 'departamento', 'seccion', 'field']);

export async function setScope(scope, ref = null, { redrawTiles = true, force = false } = {}) {
  if (!VALID_SCOPES.has(scope)) {
    diagnostics.log('warn', `setScope scope inválido: ${scope}`);
    return;
  }
  const prev = { scope: store.clipScope, ref: store.clipRef };
  // No-op si ya estaba puesto Y la máscara ya fue dibujada.
  if (!force && prev.scope === scope && prev.ref === ref && store.clipMaskLayer) return;
  setStore({ clipScope: scope, clipRef: ref });
  diagnostics.track('scope_change', { from: prev, to: { scope, ref } });

  if (store.map) {
    await applyClipMask(store.map, scope, ref);
  }

  if (redrawTiles && typeof window.redrawAllAnalyticLayers === 'function') {
    try { window.redrawAllAnalyticLayers(); } catch (err) {
      diagnostics.log('warn', `redrawAllAnalyticLayers error: ${err.message}`);
    }
  }

  // Actualizar sidebar (chip de scope)
  if (typeof window.syncSidebar === 'function') {
    try { window.syncSidebar(); } catch (_) { /* noop */ }
  }
}

export function currentScopeLabel() {
  const scope = store.clipScope || 'nacional';
  const ref = store.clipRef;
  if (scope === 'nacional') return 'Uruguay';
  if (scope === 'departamento') return `Depto · ${ref || '?'}`;
  if (scope === 'seccion') return `Sección · ${ref || '?'}`;
  if (scope === 'field') return `Campo · ${ref || '?'}`;
  return 'Sin clip';
}

export function resetToNacional() {
  return setScope('nacional', null);
}
