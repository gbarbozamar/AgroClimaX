/**
 * Modal de configuración previo al render de video timelapse.
 *
 * Flujo:
 *   1. GET /api/v1/campos/{id}/layers-available para listar capas con frames all-time.
 *   2. Muestra dropdown de capa (con count total) + hint de frames totales.
 *   3. Dropdown de duración: al cambiar, GET /timeline-frames?layer&days y
 *      mostrar el conteo REAL de frames dentro de la ventana N días.
 *   4. Si frames-en-ventana < MIN_FRAMES_FOR_VIDEO, deshabilita "Generar".
 *   5. Al confirmar, dynamic import de fieldVideo.js y abre el modal real.
 */
import { diagnostics } from './diagnostics.js?v=20260421-1';

const API_V1 = '/api/v1';
const DURATIONS = [7, 14, 30, 90, 180];
const DEFAULT_DURATION = 30;
const MIN_FRAMES_FOR_VIDEO = 2;
const VIDEO_FPS = 4;

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

async function fetchTimelineFramesCount(fieldId, layerKey, days) {
  const url =
    `${API_V1}/campos/${encodeURIComponent(fieldId)}/timeline-frames` +
    `?layer=${encodeURIComponent(layerKey)}&days=${encodeURIComponent(days)}`;
  const resp = await fetch(url, { credentials: 'same-origin' });
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`GET timeline-frames ${resp.status}: ${text.slice(0, 200)}`);
  }
  const data = await resp.json();
  // Endpoint responde { total, days: [...frames] }. Preferir 'total' por contrato,
  // fallback a length por si el backend cambia el shape.
  if (typeof data?.total === 'number') return data.total;
  if (Array.isArray(data?.days)) return data.days.length;
  return 0;
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

    // --- Layer row + hint (frames totales all-time para la capa elegida) ---
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
      opt.textContent = `${layer.label || layer.layer_key.toUpperCase()} · ${count} frames`;
      opt.dataset.count = String(count);
      layerSelect.appendChild(opt);
    });
    layerRow.appendChild(layerLabel);
    layerRow.appendChild(layerSelect);
    body.appendChild(layerRow);

    const layerHint = document.createElement('div');
    layerHint.className = 'field-video-config-hint';
    body.appendChild(layerHint);

    // --- Duration row + hint (frames reales en la ventana) ---
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

    const windowHint = document.createElement('div');
    windowHint.className = 'field-video-config-hint';
    body.appendChild(windowHint);

    // --- Total esperado (duración del video en segundos) ---
    const totalHint = document.createElement('div');
    totalHint.className = 'field-video-config-hint field-video-config-total';
    body.appendChild(totalHint);

    // --- Acciones ---
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

    actions.appendChild(cancelBtn);
    actions.appendChild(submitBtn);
    body.appendChild(actions);

    // --- Estado compartido: último conteo real (frames en ventana) ---
    let lastWindowCount = null; // null = aún no conocido / error
    // Cada llamada a refreshWindowCount incrementa este token; respuestas viejas
    // se descartan para evitar race conditions al alternar rápido entre capas.
    let inflightToken = 0;

    function selectedLayerCount() {
      const selected = layerSelect.options[layerSelect.selectedIndex];
      return Number(selected?.dataset?.count) || 0;
    }

    function renderLayerHint() {
      const count = selectedLayerCount();
      layerHint.textContent = `Frames totales para esta capa (histórico): ${count}`;
      layerHint.classList.remove('field-video-hint-error');
    }

    function renderTotalHint() {
      if (lastWindowCount == null) {
        totalHint.textContent = '';
        return;
      }
      if (lastWindowCount < MIN_FRAMES_FOR_VIDEO) {
        totalHint.textContent = '';
        return;
      }
      const seconds = (lastWindowCount / VIDEO_FPS).toFixed(1);
      totalHint.textContent =
        `Video tendrá ${lastWindowCount} frames (fps ${VIDEO_FPS}, ~${seconds}s de duración)`;
    }

    function updateSubmitState() {
      if (lastWindowCount == null) {
        submitBtn.disabled = true;
        submitBtn.title = 'Calculando frames disponibles…';
        return;
      }
      if (lastWindowCount < MIN_FRAMES_FOR_VIDEO) {
        submitBtn.disabled = true;
        submitBtn.title =
          `Se necesitan al menos ${MIN_FRAMES_FOR_VIDEO} frames en la ventana ` +
          `(hay ${lastWindowCount}). Probá una duración mayor u otra capa.`;
        return;
      }
      submitBtn.disabled = false;
      submitBtn.title = 'Generar video timelapse con la capa y duración elegidas';
    }

    async function refreshWindowCount() {
      const layerKey = layerSelect.value;
      const durationDays = Number(durationSelect.value) || DEFAULT_DURATION;
      const token = ++inflightToken;

      // Estado intermedio mientras se resuelve la request.
      lastWindowCount = null;
      windowHint.classList.remove('field-video-hint-error');
      windowHint.textContent = `Contando frames reales en los últimos ${durationDays} días…`;
      totalHint.textContent = '';
      updateSubmitState();

      try {
        const n = await fetchTimelineFramesCount(fieldId, layerKey, durationDays);
        if (token !== inflightToken) return; // otra request más nueva ya corre
        lastWindowCount = n;
        if (n < MIN_FRAMES_FOR_VIDEO) {
          windowHint.classList.add('field-video-hint-error');
          windowHint.textContent =
            `Frames reales en los últimos ${durationDays} días: ${n} — ` +
            `insuficientes para video (mínimo ${MIN_FRAMES_FOR_VIDEO})`;
        } else {
          windowHint.classList.remove('field-video-hint-error');
          windowHint.textContent =
            `Frames reales en los últimos ${durationDays} días: ${n}`;
        }
        renderTotalHint();
        updateSubmitState();
      } catch (err) {
        if (token !== inflightToken) return;
        diagnostics.log('warn', `timeline-frames count error: ${err.message}`);
        lastWindowCount = null;
        windowHint.classList.add('field-video-hint-error');
        windowHint.textContent = `No se pudo contar frames: ${err.message}`;
        totalHint.textContent = '';
        updateSubmitState();
      }
    }

    renderLayerHint();
    // Disparo inicial: contar frames para la capa/duración por defecto.
    refreshWindowCount();

    layerSelect.addEventListener('change', () => {
      renderLayerHint();
      refreshWindowCount();
    });
    durationSelect.addEventListener('change', () => {
      refreshWindowCount();
    });

    submitBtn.addEventListener('click', async () => {
      if (submitBtn.disabled) return;
      const layerKey = layerSelect.value;
      const durationDays = Number(durationSelect.value) || DEFAULT_DURATION;
      diagnostics.track('field_video_config_submit', {
        fieldId, layerKey, durationDays, framesInWindow: lastWindowCount,
      });
      close();
      const { openFieldVideoModal } = await import('./fieldVideo.js?v=20260421-1');
      openFieldVideoModal(fieldId, layerKey, durationDays);
    });
  } catch (err) {
    diagnostics.log('warn', `fieldVideoConfig error: ${err.message}`);
    showError(err.message);
  }
}
