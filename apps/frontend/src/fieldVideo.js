/**
 * Fase 4 — UI para solicitar y reproducir videos timelapse del campo.
 *
 * Flujo:
 *   1. POST /api/v1/campos/{id}/videos {layer_key, duration_days}
 *   2. Polling GET /api/v1/campos/{id}/videos/{job_id} hasta status=ready|failed
 *   3. Si ready, cargar MP4 desde job.video_url en un <video controls autoplay>
 */
import { diagnostics } from './diagnostics.js?v=20260421-1';

const API_V1 = '/api/v1';

export async function requestFieldVideo(fieldId, layerKey = 'ndvi', durationDays = 30) {
  const resp = await fetch(
    `${API_V1}/campos/${encodeURIComponent(fieldId)}/videos`,
    {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ layer_key: layerKey, duration_days: durationDays }),
    },
  );
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`POST video ${resp.status}: ${text.slice(0, 200)}`);
  }
  return await resp.json();
}

export async function pollFieldVideoStatus(fieldId, jobId, { intervalMs = 3000, timeoutMs = 300000 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const resp = await fetch(
      `${API_V1}/campos/${encodeURIComponent(fieldId)}/videos/${encodeURIComponent(jobId)}`,
      { credentials: 'same-origin' },
    );
    if (!resp.ok) throw new Error(`GET video ${resp.status}`);
    const data = await resp.json();
    if (data.status === 'ready' || data.status === 'failed') return data;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error('Video render timeout');
}

export function openFieldVideoModal(fieldId, layerKey = 'ndvi', durationDays = 30) {
  // Backdrop
  const backdrop = document.createElement('div');
  backdrop.className = 'field-video-modal-backdrop';
  backdrop.addEventListener('click', (e) => {
    if (e.target === backdrop) close();
  });

  // Panel
  const panel = document.createElement('div');
  panel.className = 'field-video-modal';

  const title = document.createElement('div');
  title.className = 'field-video-title';
  title.textContent = `Video timelapse · ${layerKey.toUpperCase()} · últimos ${durationDays} días`;

  const body = document.createElement('div');
  body.className = 'field-video-body';

  const progressText = document.createElement('div');
  progressText.className = 'field-video-progress-text';
  progressText.textContent = 'Solicitando render…';
  body.appendChild(progressText);

  const spinner = document.createElement('div');
  spinner.className = 'field-video-spinner';
  body.appendChild(spinner);

  const closeBtn = document.createElement('button');
  closeBtn.className = 'field-video-close';
  closeBtn.type = 'button';
  closeBtn.textContent = '✕';
  closeBtn.addEventListener('click', close);

  panel.appendChild(closeBtn);
  panel.appendChild(title);
  panel.appendChild(body);
  backdrop.appendChild(panel);
  document.body.appendChild(backdrop);

  function close() {
    backdrop.remove();
  }

  (async () => {
    try {
      const job = await requestFieldVideo(fieldId, layerKey, durationDays);
      diagnostics.log('info', `fieldVideo: job ${job.job_id} status=${job.status}`);
      progressText.textContent = `Render en curso (job ${job.job_id.slice(0, 8)})…`;

      // Si ya está ready (reuse de 24h), directamente mostramos.
      if (job.status === 'ready' && job.video_url) {
        showVideo(job);
        return;
      }

      const final = await pollFieldVideoStatus(fieldId, job.job_id, { intervalMs: 3000, timeoutMs: 300000 });
      if (final.status === 'ready' && final.video_url) {
        showVideo(final);
      } else {
        showError(final.error_message || 'Render falló sin detalle');
      }
    } catch (err) {
      diagnostics.log('warn', `fieldVideo error: ${err.message}`);
      showError(err.message);
    }
  })();

  function showVideo(job) {
    body.innerHTML = '';
    const video = document.createElement('video');
    video.controls = true;
    video.autoplay = true;
    video.src = job.video_url;
    video.className = 'field-video-player';
    body.appendChild(video);
    const meta = document.createElement('div');
    meta.className = 'field-video-meta';
    const frames = job.frame_count ?? job.frames ?? '?';
    const mb = Number.isFinite(job.size_bytes) ? (job.size_bytes / 1024 / 1024).toFixed(2) : '?';
    meta.textContent = `${frames} frames · ${mb} MB · layer=${layerKey} · ${durationDays}d`;
    body.appendChild(meta);
  }

  function showError(msg) {
    body.innerHTML = '';
    const err = document.createElement('div');
    err.className = 'field-video-error';
    err.textContent = `No se pudo generar el video: ${msg}`;
    body.appendChild(err);
  }
}
