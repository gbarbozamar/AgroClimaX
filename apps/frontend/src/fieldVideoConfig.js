/**
 * Modal de configuración previo al render de video timelapse.
 *
 * Flujo:
 *   1. GET /api/v1/campos/{id}/layers-available para listar capas con frames.
 *   2. Muestra dropdown de capa (con count) + dropdown de duración.
 *   3. Si la capa elegida tiene <2 frames, deshabilita "Generar" con tooltip.
 *   4. Al confirmar, dynamic import de fieldVideo.js y abre el modal real.
 */
import { diagnostics } from './diagnostics.js?v=20260421-1';

const API_V1 = '/api/v1';
const DURATIONS = [7, 14, 30, 90, 180];
const DEFAULT_DURATION = 30;
const MIN_FRAMES_FOR_VIDEO = 2;

async function fetchLayersAvailable(fieldId) {
  const resp = await fetch(
    `${API_V1}/campos/${encodeURIComponent(fieldId)}/layers-available`,
    { credentials: 'same-origin' },
  );
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`GET layers-available ${resp.status}: ${text.slice(0, 200)}`);
  }
  return await resp.json();
}

export async function openFieldVideoConfigModal(fieldId) {
  if (!fieldId) return;

  const backdrop = document.createElement('div');
  backdrop.className = 'field-video-modal-backdrop';
  backdrop.addEventListener('click', (e) => {
    if (e.target === backdrop) close();
  });

  const panel = document.createElement('div');
  panel.className = 'field-video-modal';

  const title = document.createElement('div');
  title.className = 'field-video-title';
  title.textContent = 'Configurar video timelapse';

  const body = document.createElement('div');
  body.className = 'field-video-body';

  const progressText = document.createElement('div');
  progressText.className = 'field-video-progress-text';
  progressText.textContent = 'Cargando capas disponibles…';
  body.appendChild(progressText);

  const spinner = document.createElement('div');
  spinner.className = 'field-video-spinner';
  body.appendChild(spinner);

  const closeBtn = document.createElement('button');
  closeBtn.className = 'field-video-close';
  closeBtn.type = 'button';
  closeBtn.textContent = '\u2715';
  closeBtn.addEventListener('click', close);

  panel.appendChild(closeBtn);
  panel.appendChild(title);
  panel.appendChild(body);
  backdrop.appendChild(panel);
  document.body.appendChild(backdrop);

  function close() {
    backdrop.remove();
  }

  function showError(msg) {
    body.innerHTML = '';
    const err = document.createElement('div');
    err.className = 'field-video-error';
    err.textContent = `No se pudieron cargar las capas: ${msg}`;
    body.appendChild(err);
  }

  try {
    const data = await fetchLayersAvailable(fieldId);
    const layers = Array.isArray(data?.layers) ? data.layers : [];

    body.innerHTML = '';

    if (!layers.length) {
      const empty = document.createElement('div');
      empty.className = 'field-video-error';
      empty.textContent = 'Este campo todavía no tiene frames rendereados para ninguna capa.';
      body.appendChild(empty);
      return;
    }

    // Layer row
    const layerRow = document.createElement('div');
    layerRow.className = 'field-video-config-row';
    const layerLabel = document.createElement('label');
    layerLabel.htmlFor = 'video-layer';
    layerLabel.textContent = 'Capa';
    const layerSelect = document.createElement('select');
    layerSelect.id = 'video-layer';
    layers.forEach((layer) => {
      const opt = document.createElement('option');
      opt.value = layer.layer_key;
      const count = Number(layer.count) || 0;
      opt.textContent = `${layer.label || layer.layer_key.toUpperCase()} (${count} frames)`;
      opt.dataset.count = String(count);
      layerSelect.appendChild(opt);
    });
    layerRow.appendChild(layerLabel);
    layerRow.appendChild(layerSelect);
    body.appendChild(layerRow);

    // Duration row
    const durationRow = document.createElement('div');
    durationRow.className = 'field-video-config-row';
    const durationLabel = document.createElement('label');
    durationLabel.htmlFor = 'video-duration';
    durationLabel.textContent = 'Duración';
    const durationSelect = document.createElement('select');
    durationSelect.id = 'video-duration';
    DURATIONS.forEach((days) => {
      const opt = document.createElement('option');
      opt.value = String(days);
      opt.textContent = `${days} días`;
      if (days === DEFAULT_DURATION) opt.selected = true;
      durationSelect.appendChild(opt);
    });
    durationRow.appendChild(durationLabel);
    durationRow.appendChild(durationSelect);
    body.appendChild(durationRow);

    // Actions row
    const actions = document.createElement('div');
    actions.className = 'field-video-config-actions';

    const cancelBtn = document.createElement('button');
    cancelBtn.type = 'button';
    cancelBtn.className = 'field-video-config-cancel';
    cancelBtn.textContent = 'Cancelar';
    cancelBtn.addEventListener('click', close);

    const submitBtn = document.createElement('button');
    submitBtn.type = 'button';
    submitBtn.className = 'field-video-btn field-video-config-submit';
    submitBtn.textContent = 'Generar video';

    function updateSubmitState() {
      const selected = layerSelect.options[layerSelect.selectedIndex];
      const count = Number(selected?.dataset?.count) || 0;
      if (count < MIN_FRAMES_FOR_VIDEO) {
        submitBtn.disabled = true;
        submitBtn.title = `Se necesitan al menos ${MIN_FRAMES_FOR_VIDEO} frames para generar video (esta capa tiene ${count}).`;
      } else {
        submitBtn.disabled = false;
        submitBtn.title = 'Generar video timelapse con la capa y duración elegidas';
      }
    }
    updateSubmitState();
    layerSelect.addEventListener('change', updateSubmitState);

    submitBtn.addEventListener('click', async () => {
      if (submitBtn.disabled) return;
      const layerKey = layerSelect.value;
      const durationDays = Number(durationSelect.value) || DEFAULT_DURATION;
      diagnostics.track('field_video_config_submit', {
        fieldId, layerKey, durationDays,
      });
      close();
      const { openFieldVideoModal } = await import('./fieldVideo.js?v=20260421-1');
      openFieldVideoModal(fieldId, layerKey, durationDays);
    });

    actions.appendChild(cancelBtn);
    actions.appendChild(submitBtn);
    body.appendChild(actions);
  } catch (err) {
    diagnostics.log('warn', `fieldVideoConfig error: ${err.message}`);
    showError(err.message);
  }
}
