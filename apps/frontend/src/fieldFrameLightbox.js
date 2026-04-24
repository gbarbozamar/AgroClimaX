// fieldFrameLightbox.js
// Modal lightbox para ver snapshots de campo (paddock frames) en tamaño grande.
// API:
//   openFieldFrameLightbox(frames, initialIndex, context)
//     frames       : [{observed_at, image_url, metadata:{...}}]
//     initialIndex : int (qué frame mostrar primero)
//     context      : {fieldName, layerKey}

const HEADER_ID = 'field-lightbox-header';

const STATE = {
  styles_injected: false,
  root: null,
  frames: [],
  index: 0,
  context: { fieldName: '', layerKey: '' },
  keyHandler: null,
  previousFocus: null,
  preloadCache: new Set(),
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
  outline: none;
}
.field-frame-lightbox:focus-visible {
  box-shadow: 0 24px 64px rgba(0, 0, 0, 0.6), 0 0 0 2px #1f6feb;
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
.field-frame-lightbox-close:hover,
.field-frame-lightbox-close:focus-visible {
  background: rgba(255, 255, 255, 0.08);
  outline: none;
}
.field-frame-lightbox-header {
  font-size: 0.92rem;
  font-weight: 600;
  padding-right: 32px;
  color: #cbd5e1;
  letter-spacing: 0.01em;
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}
.field-frame-lightbox-header-title {
  flex: 1 1 auto;
  min-width: 0;
}
.field-frame-lightbox-header-counter {
  flex: 0 0 auto;
  font-size: 0.8rem;
  font-weight: 500;
  color: #94a3b8;
  font-variant-numeric: tabular-nums;
  background: rgba(255, 255, 255, 0.04);
  border: 1px solid rgba(255, 255, 255, 0.07);
  border-radius: 999px;
  padding: 2px 10px;
  white-space: nowrap;
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
.field-frame-lightbox-nav:hover:not(:disabled),
.field-frame-lightbox-nav:focus-visible:not(:disabled) {
  background: rgba(0, 0, 0, 0.8);
  outline: none;
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
.field-frame-lightbox-btn:hover,
.field-frame-lightbox-btn:focus-visible {
  background: #2a7bff;
  outline: none;
}
.field-frame-lightbox-counter {
  font-size: 0.72rem;
  color: #94a3b8;
  text-align: center;
}
.field-frame-lightbox-toast {
  position: absolute;
  bottom: 18px;
  left: 50%;
  transform: translateX(-50%);
  background: rgba(0, 0, 0, 0.85);
  color: #fff;
  padding: 8px 16px;
  border-radius: 6px;
  font-size: 0.82rem;
  pointer-events: none;
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

function hasValidImageUrl(frame) {
  return !!(frame && typeof frame.image_url === 'string' && frame.image_url.trim());
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
  if (!frame || !frame.image_url) {
    showToast('Frame sin imagen disponible');
    return;
  }
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

let toastTimer = null;
function showToast(msg) {
  if (!STATE.root) return;
  let toast = STATE.root.querySelector('.field-frame-lightbox-toast');
  if (!toast) {
    toast = document.createElement('div');
    toast.className = 'field-frame-lightbox-toast';
    toast.setAttribute('role', 'status');
    toast.setAttribute('aria-live', 'polite');
    STATE.root.appendChild(toast);
  }
  toast.textContent = msg;
  if (toastTimer) clearTimeout(toastTimer);
  toastTimer = setTimeout(() => {
    if (toast && toast.parentNode) toast.parentNode.removeChild(toast);
    toastTimer = null;
  }, 2200);
}

function preloadIndex(idx) {
  if (idx < 0 || idx >= STATE.frames.length) return;
  const f = STATE.frames[idx];
  if (!hasValidImageUrl(f)) return;
  const url = f.image_url;
  if (STATE.preloadCache.has(url)) return;
  STATE.preloadCache.add(url);
  try {
    const pre = new Image();
    pre.src = url;
  } catch (_e) {
    // noop
  }
}

function findNextValidIndex(from, dir) {
  // dir: +1 or -1. Returns -1 if none.
  let i = from;
  while (i >= 0 && i < STATE.frames.length) {
    if (hasValidImageUrl(STATE.frames[i])) return i;
    i += dir;
  }
  return -1;
}

function ensureValidIndex() {
  // If current frame lacks image_url, try to skip forward, then backward.
  if (hasValidImageUrl(STATE.frames[STATE.index])) return true;
  const fwd = findNextValidIndex(STATE.index + 1, +1);
  if (fwd !== -1) {
    STATE.index = fwd;
    return true;
  }
  const back = findNextValidIndex(STATE.index - 1, -1);
  if (back !== -1) {
    STATE.index = back;
    return true;
  }
  return false;
}

function renderFrame() {
  if (!STATE.root) return;
  const frame = currentFrame();
  const modal = STATE.root.querySelector('.field-frame-lightbox');
  if (!modal || !frame) return;

  // Header
  const titleEl = modal.querySelector('.field-frame-lightbox-header-title');
  const counterEl = modal.querySelector('.field-frame-lightbox-header-counter');
  const observed = frame.observed_at || '';
  const layerLabel = (STATE.context.layerKey || '').toUpperCase();
  if (titleEl) {
    titleEl.textContent = `${STATE.context.fieldName || ''} · ${layerLabel} · ${observed}`;
  }
  if (counterEl) {
    counterEl.textContent = `${STATE.index + 1} / ${STATE.frames.length}`;
  }

  // Image (fade-in)
  const img = modal.querySelector('.field-frame-lightbox-img');
  img.classList.add('is-loading');
  img.alt = `${STATE.context.fieldName || 'field'} ${layerLabel} ${observed}`.trim();
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

  // Counter (footer, compact)
  const counter = modal.querySelector('.field-frame-lightbox-counter');
  if (counter) {
    counter.textContent = STATE.frames.length > 1
      ? `${STATE.index + 1} / ${STATE.frames.length}`
      : '';
  }

  // Meta
  const metaWrap = modal.querySelector('.field-frame-lightbox-meta-wrap');
  metaWrap.replaceChildren(renderMeta(frame));

  // Preload neighbors so next arrow press is instant.
  preloadIndex(STATE.index + 1);
  preloadIndex(STATE.index - 1);
}

function goTo(delta) {
  const total = STATE.frames.length;
  if (!total) return;
  let next = STATE.index + delta;
  if (next < 0) next = 0;
  if (next >= total) next = total - 1;
  if (next === STATE.index) return;

  // If the target frame has no image_url, try to skip over it in the same direction.
  if (!hasValidImageUrl(STATE.frames[next])) {
    const dir = delta >= 0 ? +1 : -1;
    const valid = findNextValidIndex(next, dir);
    if (valid !== -1) {
      next = valid;
    } else {
      // No more valid frames in this direction; stay put.
      showToast('Sin más frames disponibles');
      return;
    }
  }

  STATE.index = next;
  renderFrame();
}

function goToIndex(idx) {
  if (!STATE.frames.length) return;
  let target = Math.max(0, Math.min(idx, STATE.frames.length - 1));
  if (!hasValidImageUrl(STATE.frames[target])) {
    const dir = idx >= STATE.index ? +1 : -1;
    const valid = findNextValidIndex(target, dir);
    if (valid !== -1) target = valid;
    else {
      showToast('Sin más frames disponibles');
      return;
    }
  }
  if (target === STATE.index) return;
  STATE.index = target;
  renderFrame();
}

function getFocusableElements(container) {
  if (!container) return [];
  const sel = [
    'a[href]',
    'button:not([disabled])',
    'input:not([disabled])',
    'select:not([disabled])',
    'textarea:not([disabled])',
    '[tabindex]:not([tabindex="-1"])',
  ].join(',');
  return Array.from(container.querySelectorAll(sel)).filter(
    (el) => !el.hasAttribute('disabled') && el.offsetParent !== null,
  );
}

function trapFocus(ev) {
  if (!STATE.root) return;
  const modal = STATE.root.querySelector('.field-frame-lightbox');
  if (!modal) return;
  const focusables = getFocusableElements(modal);
  if (!focusables.length) {
    ev.preventDefault();
    modal.focus();
    return;
  }
  const first = focusables[0];
  const last = focusables[focusables.length - 1];
  const active = document.activeElement;
  if (ev.shiftKey) {
    if (active === first || active === modal || !modal.contains(active)) {
      ev.preventDefault();
      last.focus();
    }
  } else {
    if (active === last) {
      ev.preventDefault();
      first.focus();
    }
  }
}

function handleKeydown(ev) {
  if (!STATE.root) return;
  const key = ev.key;

  if (key === 'Escape') {
    ev.preventDefault();
    closeLightbox();
    return;
  }
  if (key === 'ArrowLeft') {
    ev.preventDefault();
    goTo(-1);
    return;
  }
  if (key === 'ArrowRight') {
    ev.preventDefault();
    goTo(+1);
    return;
  }
  if (key === 'Home') {
    ev.preventDefault();
    goToIndex(0);
    return;
  }
  if (key === 'End') {
    ev.preventDefault();
    goToIndex(STATE.frames.length - 1);
    return;
  }
  if (key === 'd' || key === 'D') {
    // Don't intercept if user is typing in an input (future-proof).
    const tgt = ev.target;
    const tag = tgt && tgt.tagName ? tgt.tagName.toLowerCase() : '';
    if (tag === 'input' || tag === 'textarea' || (tgt && tgt.isContentEditable)) return;
    ev.preventDefault();
    triggerDownload(currentFrame());
    return;
  }
  if (key === 'Tab') {
    trapFocus(ev);
    return;
  }
}

function closeLightbox() {
  if (STATE.keyHandler) {
    document.removeEventListener('keydown', STATE.keyHandler);
    STATE.keyHandler = null;
  }
  if (toastTimer) {
    clearTimeout(toastTimer);
    toastTimer = null;
  }
  if (STATE.root && STATE.root.parentNode) {
    STATE.root.parentNode.removeChild(STATE.root);
  }
  STATE.root = null;
  STATE.frames = [];
  STATE.index = 0;
  STATE.preloadCache = new Set();

  // Restore focus to the element that had it before opening.
  const prev = STATE.previousFocus;
  STATE.previousFocus = null;
  if (prev && typeof prev.focus === 'function' && document.contains(prev)) {
    try {
      prev.focus();
    } catch (_e) {
      // ignore
    }
  }
}

function buildDOM() {
  const backdrop = document.createElement('div');
  backdrop.className = 'field-frame-lightbox-backdrop';

  const modal = document.createElement('div');
  modal.className = 'field-frame-lightbox';
  modal.setAttribute('role', 'dialog');
  modal.setAttribute('aria-modal', 'true');
  modal.setAttribute('aria-labelledby', HEADER_ID);
  modal.setAttribute('tabindex', '-1');

  const closeBtn = document.createElement('button');
  closeBtn.type = 'button';
  closeBtn.className = 'field-frame-lightbox-close';
  closeBtn.setAttribute('aria-label', 'Cerrar');
  closeBtn.textContent = '\u2715'; // ✕
  modal.appendChild(closeBtn);

  const header = document.createElement('div');
  header.className = 'field-frame-lightbox-header';
  header.id = HEADER_ID;
  const headerTitle = document.createElement('span');
  headerTitle.className = 'field-frame-lightbox-header-title';
  const headerCounter = document.createElement('span');
  headerCounter.className = 'field-frame-lightbox-header-counter';
  headerCounter.setAttribute('aria-live', 'polite');
  headerCounter.setAttribute('aria-atomic', 'true');
  header.appendChild(headerTitle);
  header.appendChild(headerCounter);
  modal.appendChild(header);

  const stage = document.createElement('div');
  stage.className = 'field-frame-lightbox-stage';

  const prevBtn = document.createElement('button');
  prevBtn.type = 'button';
  prevBtn.className = 'field-frame-lightbox-nav field-frame-lightbox-nav-left';
  prevBtn.setAttribute('aria-label', 'Anterior frame');
  prevBtn.textContent = '\u2190'; // ←
  stage.appendChild(prevBtn);

  const img = document.createElement('img');
  img.className = 'field-frame-lightbox-img';
  img.alt = '';
  stage.appendChild(img);

  const nextBtn = document.createElement('button');
  nextBtn.type = 'button';
  nextBtn.className = 'field-frame-lightbox-nav field-frame-lightbox-nav-right';
  nextBtn.setAttribute('aria-label', 'Siguiente frame');
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
  dlBtn.setAttribute('aria-label', 'Descargar PNG');
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
  STATE.preloadCache = new Set();

  // Fallback: if initial frame has no image_url, jump to the closest valid one.
  if (!ensureValidIndex()) {
    console.warn('[fieldFrameLightbox] no frames with valid image_url; aborting open');
    STATE.frames = [];
    STATE.index = 0;
    return;
  }

  // Preserve the currently focused element so we can restore it on close.
  STATE.previousFocus = (document && document.activeElement) || null;

  const root = buildDOM();
  document.body.appendChild(root);
  STATE.root = root;

  STATE.keyHandler = handleKeydown;
  document.addEventListener('keydown', STATE.keyHandler);

  renderFrame();

  // After the DOM is in place, move focus to the dialog for screenreaders + keyboard.
  const modal = root.querySelector('.field-frame-lightbox');
  if (modal && typeof modal.focus === 'function') {
    // next tick: make sure layout is done so focus ring renders on the right element.
    requestAnimationFrame(() => {
      try {
        modal.focus({ preventScroll: true });
      } catch (_e) {
        modal.focus();
      }
    });
  }
}

export default openFieldFrameLightbox;
