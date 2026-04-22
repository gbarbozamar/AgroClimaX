// fieldFrameLightbox.js
// Modal lightbox para ver snapshots de campo (paddock frames) en tamaño grande.
// API:
//   openFieldFrameLightbox(frames, initialIndex, context)
//     frames       : [{observed_at, image_url, metadata:{...}}]
//     initialIndex : int (qué frame mostrar primero)
//     context      : {fieldName, layerKey}

const STATE = {
  styles_injected: false,
  root: null,
  frames: [],
  index: 0,
  context: { fieldName: '', layerKey: '' },
  keyHandler: null,
};

const STYLES = `
.field-frame-lightbox-backdrop {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.85);
  z-index: 10500;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
  box-sizing: border-box;
}
.field-frame-lightbox {
  position: relative;
  background: #14181f;
  color: #e6edf3;
  border-radius: 12px;
  padding: 22px;
  max-width: 90vw;
  max-height: 92vh;
  overflow: auto;
  box-shadow: 0 24px 64px rgba(0, 0, 0, 0.6);
  font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.field-frame-lightbox-close {
  position: absolute;
  top: 8px;
  right: 10px;
  background: transparent;
  color: #e6edf3;
  border: none;
  font-size: 1.3rem;
  cursor: pointer;
  padding: 4px 8px;
  line-height: 1;
  border-radius: 6px;
}
.field-frame-lightbox-close:hover {
  background: rgba(255, 255, 255, 0.08);
}
.field-frame-lightbox-header {
  font-size: 0.92rem;
  font-weight: 600;
  padding-right: 32px;
  color: #cbd5e1;
  letter-spacing: 0.01em;
}
.field-frame-lightbox-stage {
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 200px;
}
.field-frame-lightbox-img {
  max-width: 80vw;
  max-height: 65vh;
  object-fit: contain;
  border-radius: 8px;
  background: #0a0d12;
  transition: opacity 120ms ease;
}
.field-frame-lightbox-img.is-loading {
  opacity: 0;
}
.field-frame-lightbox-nav {
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
  background: rgba(0, 0, 0, 0.55);
  color: #fff;
  border: 1px solid rgba(255, 255, 255, 0.15);
  width: 42px;
  height: 42px;
  border-radius: 50%;
  font-size: 1.2rem;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  user-select: none;
}
.field-frame-lightbox-nav:hover:not(:disabled) {
  background: rgba(0, 0, 0, 0.8);
}
.field-frame-lightbox-nav:disabled {
  opacity: 0.3;
  cursor: default;
}
.field-frame-lightbox-nav-left {
  left: -8px;
}
.field-frame-lightbox-nav-right {
  right: -8px;
}
.field-frame-lightbox-meta {
  display: grid;
  grid-template-columns: max-content 1fr;
  gap: 4px 12px;
  font-size: 0.78rem;
  color: #cbd5e1;
  background: rgba(255, 255, 255, 0.03);
  border: 1px solid rgba(255, 255, 255, 0.06);
  border-radius: 8px;
  padding: 10px 14px;
}
.field-frame-lightbox-meta dt {
  color: #94a3b8;
  font-weight: 500;
}
.field-frame-lightbox-meta dd {
  margin: 0;
  color: #e6edf3;
  font-variant-numeric: tabular-nums;
  word-break: break-word;
}
.field-frame-lightbox-actions {
  display: flex;
  flex-direction: row;
  gap: 8px;
  justify-content: flex-end;
}
.field-frame-lightbox-btn {
  background: #1f6feb;
  color: #fff;
  border: none;
  border-radius: 6px;
  padding: 8px 14px;
  font-size: 0.82rem;
  cursor: pointer;
}
.field-frame-lightbox-btn:hover {
  background: #2a7bff;
}
.field-frame-lightbox-counter {
  font-size: 0.72rem;
  color: #94a3b8;
  text-align: center;
}
`;

function injectStyles() {
  if (STATE.styles_injected) return;
  const style = document.createElement('style');
  style.setAttribute('data-field-frame-lightbox', '1');
  style.textContent = STYLES;
  document.head.appendChild(style);
  STATE.styles_injected = true;
}

function slugify(str) {
  return String(str || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '') || 'frame';
}

function formatNumber(n, digits = 3) {
  if (n === null || n === undefined || Number.isNaN(n)) return null;
  const num = Number(n);
  if (!Number.isFinite(num)) return null;
  if (Math.abs(num) >= 1000) return num.toFixed(0);
  return num.toFixed(digits).replace(/\.?0+$/, '');
}

function formatBBox(bbox) {
  if (!bbox) return null;
  if (Array.isArray(bbox) && bbox.length >= 4) {
    return bbox.slice(0, 4).map((v) => formatNumber(v, 5) ?? String(v)).join(', ');
  }
  if (typeof bbox === 'object') {
    const keys = ['minx', 'miny', 'maxx', 'maxy', 'west', 'south', 'east', 'north'];
    const parts = keys.filter((k) => k in bbox).map((k) => `${k}=${formatNumber(bbox[k], 5) ?? bbox[k]}`);
    if (parts.length) return parts.join(', ');
  }
  return String(bbox);
}

function currentFrame() {
  return STATE.frames[STATE.index] || null;
}

function buildMetaRows(frame) {
  const rows = [];
  const meta = (frame && frame.metadata) || {};
  rows.push(['Fecha', frame && frame.observed_at ? String(frame.observed_at) : '—']);
  if ('risk_score' in meta && meta.risk_score !== null && meta.risk_score !== undefined) {
    const v = formatNumber(meta.risk_score, 3);
    rows.push(['Risk score', v !== null ? v : String(meta.risk_score)]);
  }
  if ('s2_ndmi_mean' in meta && meta.s2_ndmi_mean !== null && meta.s2_ndmi_mean !== undefined) {
    const v = formatNumber(meta.s2_ndmi_mean, 4);
    rows.push(['NDMI', v !== null ? v : String(meta.s2_ndmi_mean)]);
  }
  if ('area_ha' in meta && meta.area_ha !== null && meta.area_ha !== undefined) {
    const v = formatNumber(meta.area_ha, 2);
    rows.push(['Área', `${v !== null ? v : meta.area_ha} ha`]);
  }
  if ('bbox' in meta && meta.bbox) {
    const v = formatBBox(meta.bbox);
    if (v) rows.push(['BBox', v]);
  }
  if ('width_px' in meta && 'height_px' in meta && meta.width_px && meta.height_px) {
    rows.push(['Tamaño', `${meta.width_px} x ${meta.height_px} px`]);
  }
  return rows;
}

function renderMeta(frame) {
  const dl = document.createElement('dl');
  dl.className = 'field-frame-lightbox-meta';
  const rows = buildMetaRows(frame);
  for (const [k, v] of rows) {
    const dt = document.createElement('dt');
    dt.textContent = k;
    const dd = document.createElement('dd');
    dd.textContent = v;
    dl.appendChild(dt);
    dl.appendChild(dd);
  }
  return dl;
}

function downloadFilename(frame) {
  const fieldSlug = slugify(STATE.context.fieldName || 'field');
  const layerSlug = slugify(STATE.context.layerKey || 'layer');
  const raw = frame && frame.observed_at ? String(frame.observed_at) : 'frame';
  const dateSlug = slugify(raw.slice(0, 19));
  return `${fieldSlug}-${layerSlug}-${dateSlug}.png`;
}

function triggerDownload(frame) {
  if (!frame || !frame.image_url) return;
  const a = document.createElement('a');
  a.href = frame.image_url;
  a.download = downloadFilename(frame);
  a.rel = 'noopener';
  a.style.display = 'none';
  document.body.appendChild(a);
  a.click();
  setTimeout(() => {
    if (a.parentNode) a.parentNode.removeChild(a);
  }, 0);
}

function renderFrame() {
  if (!STATE.root) return;
  const frame = currentFrame();
  const modal = STATE.root.querySelector('.field-frame-lightbox');
  if (!modal || !frame) return;

  // Header
  const header = modal.querySelector('.field-frame-lightbox-header');
  const observed = frame.observed_at || '';
  const layerLabel = (STATE.context.layerKey || '').toUpperCase();
  header.textContent = `${STATE.context.fieldName || ''} · ${layerLabel} · ${observed}`;

  // Image (fade-in)
  const img = modal.querySelector('.field-frame-lightbox-img');
  img.classList.add('is-loading');
  img.alt = `${STATE.context.fieldName || 'field'} ${observed}`;
  const onLoad = () => {
    img.classList.remove('is-loading');
    img.removeEventListener('load', onLoad);
  };
  img.addEventListener('load', onLoad);
  img.src = frame.image_url || '';
  // If image is cached, load fires synchronously before listener in some browsers; force next-frame.
  requestAnimationFrame(() => {
    if (img.complete && img.naturalWidth > 0) {
      img.classList.remove('is-loading');
    }
  });

  // Nav buttons
  const prevBtn = modal.querySelector('.field-frame-lightbox-nav-left');
  const nextBtn = modal.querySelector('.field-frame-lightbox-nav-right');
  prevBtn.disabled = STATE.index <= 0;
  nextBtn.disabled = STATE.index >= STATE.frames.length - 1;

  // Counter
  const counter = modal.querySelector('.field-frame-lightbox-counter');
  if (counter) {
    counter.textContent = STATE.frames.length > 1
      ? `${STATE.index + 1} / ${STATE.frames.length}`
      : '';
  }

  // Meta
  const metaWrap = modal.querySelector('.field-frame-lightbox-meta-wrap');
  metaWrap.replaceChildren(renderMeta(frame));
}

function goTo(delta) {
  const next = STATE.index + delta;
  if (next < 0 || next >= STATE.frames.length) return;
  STATE.index = next;
  renderFrame();
}

function handleKeydown(ev) {
  if (ev.key === 'Escape') {
    ev.preventDefault();
    closeLightbox();
  } else if (ev.key === 'ArrowLeft') {
    ev.preventDefault();
    goTo(-1);
  } else if (ev.key === 'ArrowRight') {
    ev.preventDefault();
    goTo(+1);
  }
}

function closeLightbox() {
  if (STATE.keyHandler) {
    document.removeEventListener('keydown', STATE.keyHandler);
    STATE.keyHandler = null;
  }
  if (STATE.root && STATE.root.parentNode) {
    STATE.root.parentNode.removeChild(STATE.root);
  }
  STATE.root = null;
  STATE.frames = [];
  STATE.index = 0;
}

function buildDOM() {
  const backdrop = document.createElement('div');
  backdrop.className = 'field-frame-lightbox-backdrop';

  const modal = document.createElement('div');
  modal.className = 'field-frame-lightbox';
  modal.setAttribute('role', 'dialog');
  modal.setAttribute('aria-modal', 'true');

  const closeBtn = document.createElement('button');
  closeBtn.type = 'button';
  closeBtn.className = 'field-frame-lightbox-close';
  closeBtn.setAttribute('aria-label', 'Cerrar');
  closeBtn.textContent = '\u2715'; // ✕
  modal.appendChild(closeBtn);

  const header = document.createElement('div');
  header.className = 'field-frame-lightbox-header';
  modal.appendChild(header);

  const stage = document.createElement('div');
  stage.className = 'field-frame-lightbox-stage';

  const prevBtn = document.createElement('button');
  prevBtn.type = 'button';
  prevBtn.className = 'field-frame-lightbox-nav field-frame-lightbox-nav-left';
  prevBtn.setAttribute('aria-label', 'Frame anterior');
  prevBtn.textContent = '\u2190'; // ←
  stage.appendChild(prevBtn);

  const img = document.createElement('img');
  img.className = 'field-frame-lightbox-img';
  img.alt = '';
  stage.appendChild(img);

  const nextBtn = document.createElement('button');
  nextBtn.type = 'button';
  nextBtn.className = 'field-frame-lightbox-nav field-frame-lightbox-nav-right';
  nextBtn.setAttribute('aria-label', 'Frame siguiente');
  nextBtn.textContent = '\u2192'; // →
  stage.appendChild(nextBtn);

  modal.appendChild(stage);

  const counter = document.createElement('div');
  counter.className = 'field-frame-lightbox-counter';
  modal.appendChild(counter);

  const metaWrap = document.createElement('div');
  metaWrap.className = 'field-frame-lightbox-meta-wrap';
  modal.appendChild(metaWrap);

  const actions = document.createElement('div');
  actions.className = 'field-frame-lightbox-actions';
  const dlBtn = document.createElement('button');
  dlBtn.type = 'button';
  dlBtn.className = 'field-frame-lightbox-btn';
  dlBtn.textContent = '\u2B07 Descargar PNG'; // ⬇
  actions.appendChild(dlBtn);
  modal.appendChild(actions);

  backdrop.appendChild(modal);

  // Wire events
  closeBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    closeLightbox();
  });
  backdrop.addEventListener('click', (ev) => {
    if (ev.target === backdrop) closeLightbox();
  });
  modal.addEventListener('click', (ev) => ev.stopPropagation());
  prevBtn.addEventListener('click', (ev) => {
    ev.stopPropagation();
    goTo(-1);
  });
  nextBtn.addEventListener('click', (ev) => {
    ev.stopPropagation();
    goTo(+1);
  });
  dlBtn.addEventListener('click', (ev) => {
    ev.stopPropagation();
    triggerDownload(currentFrame());
  });

  return backdrop;
}

export function openFieldFrameLightbox(frames, initialIndex, context) {
  if (!Array.isArray(frames) || frames.length === 0) {
    console.warn('[fieldFrameLightbox] no frames to display');
    return;
  }
  // If a previous instance is still open, tear it down cleanly before opening again.
  if (STATE.root) {
    closeLightbox();
  }

  injectStyles();

  const idx = Number.isInteger(initialIndex) ? initialIndex : 0;
  STATE.frames = frames.slice();
  STATE.index = Math.max(0, Math.min(idx, STATE.frames.length - 1));
  STATE.context = {
    fieldName: (context && context.fieldName) || '',
    layerKey: (context && context.layerKey) || '',
  };

  const root = buildDOM();
  document.body.appendChild(root);
  STATE.root = root;

  STATE.keyHandler = handleKeydown;
  document.addEventListener('keydown', STATE.keyHandler);

  renderFrame();
}

export default openFieldFrameLightbox;
