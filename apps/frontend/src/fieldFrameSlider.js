/**
 * Field Frame Slider — componente aislado para renderizar un slider
 * horizontal de frames del timeline de un campo (paddock).
 *
 * Cada frame es { observed_at, image_url, metadata } y se pinta como un
 * "dot" con thumbnail + fecha debajo. Al clickear dispara onSelect(frame).
 *
 * Uso:
 *   import {
 *     renderFieldFrameSlider,
 *     injectFieldFrameSliderStyles,
 *   } from './fieldFrameSlider.js?v=20260421-2';
 *
 *   injectFieldFrameSliderStyles();
 *   renderFieldFrameSlider(containerEl, frames, {
 *     onSelect: (frame) => { ... },
 *     selectedDate: '2026-04-18T00:00:00Z',
 *     layerKey: 'ndvi',
 *   });
 */

const STYLE_TAG_ID = 'field-frame-slider-styles';
const COLLAPSED_STORAGE_KEY = 'agroclimax:field-slider-collapsed';

function readCollapsedState() {
  try {
    if (typeof localStorage === 'undefined') return false;
    return localStorage.getItem(COLLAPSED_STORAGE_KEY) === 'true';
  } catch (_err) {
    return false;
  }
}

function writeCollapsedState(value) {
  try {
    if (typeof localStorage === 'undefined') return;
    localStorage.setItem(COLLAPSED_STORAGE_KEY, value ? 'true' : 'false');
  } catch (_err) {
    // Ignoramos si localStorage no está disponible (e.g. modo privado).
  }
}

const CSS = `
.field-frame-slider {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 8px 10px;
  background: rgba(20, 24, 32, 0.92);
  color: #e8edf3;
  border-radius: 8px;
  font-family: system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif;
  font-size: 12px;
  box-sizing: border-box;
  max-width: 100%;
}
.field-frame-slider-header {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  gap: 12px;
  padding: 0 2px;
}
.field-frame-slider-title {
  font-weight: 600;
  font-size: 12px;
  color: #cfd6df;
  letter-spacing: 0.02em;
}
.field-frame-slider-date {
  font-variant-numeric: tabular-nums;
  font-size: 11px;
  color: #f5a623;
  font-weight: 500;
}
.field-frame-slider-track {
  display: flex;
  flex-direction: row;
  gap: 6px;
  overflow-x: auto;
  overflow-y: hidden;
  padding: 4px 2px 8px 2px;
  scroll-behavior: smooth;
  scrollbar-width: thin;
  scrollbar-color: #3a4250 transparent;
}
.field-frame-slider-track::-webkit-scrollbar {
  height: 6px;
}
.field-frame-slider-track::-webkit-scrollbar-thumb {
  background: #3a4250;
  border-radius: 3px;
}
.field-frame-slider-empty {
  padding: 10px 4px;
  color: #8a93a0;
  font-style: italic;
}
.field-frame-dot {
  flex: 0 0 auto;
  width: 80px;
  height: 90px;
  padding: 0;
  margin: 0;
  display: flex;
  flex-direction: column;
  align-items: stretch;
  justify-content: flex-start;
  background: #1b1f27;
  border: 1.5px solid #2a3140;
  border-radius: 6px;
  cursor: pointer;
  overflow: hidden;
  box-sizing: border-box;
  transition: transform 120ms ease, border-color 120ms ease, box-shadow 120ms ease;
  color: inherit;
  font-family: inherit;
}
.field-frame-dot:hover {
  border-color: #4b5466;
  transform: translateY(-1px);
}
.field-frame-dot:focus-visible {
  outline: 2px solid #f5a623;
  outline-offset: 1px;
}
.field-frame-dot.active {
  border-color: #f5a623;
  box-shadow: 0 0 0 1px #f5a623 inset, 0 2px 6px rgba(245, 166, 35, 0.25);
  transform: scale(1.08);
  z-index: 1;
}
.field-frame-thumb {
  width: 80px;
  height: 64px;
  object-fit: cover;
  display: block;
  background: #0d1016;
  border-bottom: 1px solid #2a3140;
  flex: 0 0 64px;
}
.field-frame-thumb-missing {
  width: 80px;
  height: 64px;
  background: repeating-linear-gradient(
    45deg,
    #1a1e26 0 6px,
    #232833 6px 12px
  );
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 10px;
  color: #6b7380;
  border-bottom: 1px solid #2a3140;
  flex: 0 0 64px;
}
.field-frame-date {
  display: block;
  font-size: 10px;
  line-height: 1;
  padding: 5px 4px;
  text-align: center;
  font-variant-numeric: tabular-nums;
  color: #cfd6df;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.field-frame-dot.active .field-frame-date {
  color: #ffd27d;
  font-weight: 600;
}
.field-frame-slider-layer-select {
  background: rgba(255,255,255,0.05);
  color: #e8e8e8;
  border: 1px solid rgba(255,255,255,0.15);
  border-radius: 6px;
  padding: 4px 8px;
  font-size: 0.72rem;
  cursor: pointer;
  margin-left: 12px;
}
.field-frame-slider-layer-select:hover {
  background: rgba(255,255,255,0.1);
}
.field-frame-slider.collapsed .field-frame-slider-track { display: none; }
.field-frame-slider.collapsed .field-frame-slider-toggle { transform: rotate(-90deg); }
.field-frame-slider-toggle {
  background: transparent;
  border: none;
  color: #9aa0b0;
  cursor: pointer;
  font-size: 1rem;
  padding: 0 8px;
  transition: transform 0.15s ease;
}
.field-frame-slider-toggle:hover { color: #fff; }
.field-frame-dot { position: relative; }
.field-frame-download {
  position: absolute;
  top: 2px; right: 2px;
  background: rgba(0, 0, 0, 0.6);
  color: #fff;
  border: none;
  border-radius: 4px;
  padding: 2px 6px;
  font-size: 0.7rem;
  cursor: pointer;
  opacity: 0;
  transition: opacity 0.15s;
  z-index: 2;
}
.field-frame-dot:hover .field-frame-download { opacity: 1; }
.field-frame-download:hover { background: rgba(0, 0, 0, 0.85); }
`;

/**
 * Inyecta las reglas CSS una única vez en <head>. Idempotente.
 */
export function injectFieldFrameSliderStyles() {
  if (typeof document === 'undefined') return;
  if (document.getElementById(STYLE_TAG_ID)) return;
  const style = document.createElement('style');
  style.id = STYLE_TAG_ID;
  style.type = 'text/css';
  style.appendChild(document.createTextNode(CSS));
  document.head.appendChild(style);
}

function formatShortDate(iso) {
  if (!iso) return '';
  // Tomamos los primeros 10 chars (YYYY-MM-DD). Si viene un Date, lo convertimos.
  try {
    if (typeof iso === 'string') {
      return iso.slice(0, 10);
    }
    if (iso instanceof Date) {
      return iso.toISOString().slice(0, 10);
    }
    return String(iso).slice(0, 10);
  } catch (_err) {
    return String(iso);
  }
}

function sameDate(a, b) {
  if (!a || !b) return false;
  return formatShortDate(a) === formatShortDate(b);
}

function escapeAttr(value) {
  if (value == null) return '';
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function sanitizeFilenamePart(value, fallback = '') {
  if (value == null) return fallback;
  const cleaned = String(value)
    .replace(/[\\/:*?"<>|]+/g, '-')
    .replace(/\s+/g, '_')
    .replace(/-+/g, '-')
    .replace(/^[-_\s]+|[-_\s]+$/g, '');
  return cleaned || fallback;
}

function attachToggleHandler(containerEl) {
  const root = containerEl.querySelector('.field-frame-slider');
  const btn = containerEl.querySelector('[data-role="toggle-slider"]');
  if (!root || !btn) return;
  btn.addEventListener('click', (ev) => {
    ev.preventDefault();
    ev.stopPropagation();
    const willCollapse = !root.classList.contains('collapsed');
    root.classList.toggle('collapsed', willCollapse);
    writeCollapsedState(willCollapse);
  });
}

function buildLayerSelectHtml(availableLayers, selectedKey) {
  if (!Array.isArray(availableLayers) || availableLayers.length === 0) return '';
  const optionsHtml = availableLayers
    .map((layer) => {
      if (!layer || !layer.layer_key) return '';
      const label = layer.label || String(layer.layer_key).toUpperCase();
      const count = Number.isFinite(Number(layer.count)) ? Number(layer.count) : 0;
      const optionLabel = `${label} · ${count} frame${count === 1 ? '' : 's'}`;
      const isSelected = layer.layer_key === selectedKey ? ' selected' : '';
      return `<option value="${escapeAttr(layer.layer_key)}"${isSelected}>${escapeAttr(
        optionLabel,
      )}</option>`;
    })
    .join('');
  return `<select class="field-frame-slider-layer-select" data-role="layer-select">${optionsHtml}</select>`;
}

function attachLayerSelectHandler(containerEl, onLayerChange) {
  if (typeof onLayerChange !== 'function') return;
  const select = containerEl.querySelector('[data-role="layer-select"]');
  if (!select) return;
  select.addEventListener('change', (ev) => {
    const target = ev?.target;
    const newKey = target?.value;
    if (!newKey) return;
    try {
      onLayerChange(newKey);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.warn('fieldFrameSlider onLayerChange error:', err);
    }
  });
}

function buildTooltip(frame) {
  const date = formatShortDate(frame?.observed_at);
  const risk =
    frame?.metadata && (frame.metadata.risk_score ?? frame.metadata.risk);
  if (risk != null && !Number.isNaN(Number(risk))) {
    const r = Number(risk);
    return `${date} · risk ${r.toFixed ? r.toFixed(2) : r}`;
  }
  return date;
}

/**
 * Renderiza el slider dentro de containerEl (reemplazando su contenido).
 *
 * @param {HTMLElement} containerEl
 * @param {Array<{observed_at: string, image_url: string, metadata?: object}>} frames
 * @param {object} [opts]
 * @param {(frame: object) => void} [opts.onSelect]
 * @param {string} [opts.selectedDate]   ISO date del frame activo
 * @param {string} [opts.layerKey]       e.g. 'ndvi', 'ndwi' (opcional, para el título)
 * @param {Array<{layer_key: string, count: number, label?: string}>} [opts.availableLayers]
 *        Lista de capas con snapshots disponibles, output de /layers-available.
 * @param {(newLayerKey: string) => void} [opts.onLayerChange]
 *        Callback invocado al cambiar de capa desde el dropdown del header.
 */
export function renderFieldFrameSlider(containerEl, frames, opts = {}) {
  if (!containerEl || !(containerEl instanceof HTMLElement)) return;
  injectFieldFrameSliderStyles();

  const { onSelect, selectedDate, layerKey, availableLayers, onLayerChange } = opts || {};
  const list = Array.isArray(frames) ? frames : [];
  const collapsed = readCollapsedState();
  const collapsedClass = collapsed ? ' collapsed' : '';
  const layerSelectHtml = buildLayerSelectHtml(availableLayers, layerKey);

  // Si no hay frames, pintamos estado vacío.
  if (list.length === 0) {
    containerEl.innerHTML = `
      <div class="field-frame-slider${collapsedClass}">
        <div class="field-frame-slider-header">
          <span class="field-frame-slider-title">Timeline del campo${
            layerKey ? ` · ${escapeAttr(layerKey)}` : ''
          } · 0 frames</span>
          ${layerSelectHtml}
          <span class="field-frame-slider-date"></span>
          <button type="button" class="field-frame-slider-toggle" data-role="toggle-slider" title="Plegar/expandir">▾</button>
        </div>
        <div class="field-frame-slider-empty">Sin snapshots disponibles para este campo.</div>
      </div>
    `;
    attachToggleHandler(containerEl);
    attachLayerSelectHandler(containerEl, onLayerChange);
    return;
  }

  // Determinamos el frame activo.
  const activeDate = selectedDate || list[list.length - 1]?.observed_at || null;
  const activeFrame =
    list.find((f) => sameDate(f?.observed_at, activeDate)) ||
    list[list.length - 1];

  const titleText = `Timeline del campo${
    layerKey ? ` · ${escapeAttr(layerKey)}` : ''
  } · ${list.length} frame${list.length === 1 ? '' : 's'}`;

  const headerDateText = formatShortDate(activeFrame?.observed_at);

  const dotsHtml = list
    .map((frame) => {
      const date = formatShortDate(frame?.observed_at);
      const tooltip = buildTooltip(frame);
      const isActive = sameDate(frame?.observed_at, activeDate);
      const thumb = frame?.image_url
        ? `<img src="${escapeAttr(
            frame.image_url,
          )}" alt="${escapeAttr(date)}" class="field-frame-thumb" loading="lazy" />`
        : `<div class="field-frame-thumb-missing" aria-label="sin imagen">n/a</div>`;
      return `
        <button
          type="button"
          class="field-frame-dot${isActive ? ' active' : ''}"
          data-observed="${escapeAttr(frame?.observed_at || '')}"
          title="${escapeAttr(tooltip)}"
        >
          ${thumb}
          <span class="field-frame-date">${escapeAttr(date)}</span>
        </button>
      `;
    })
    .join('');

  containerEl.innerHTML = `
    <div class="field-frame-slider${collapsedClass}">
      <div class="field-frame-slider-header">
        <span class="field-frame-slider-title">${titleText}</span>
        ${layerSelectHtml}
        <span class="field-frame-slider-date">${escapeAttr(headerDateText)}</span>
        <button type="button" class="field-frame-slider-toggle" data-role="toggle-slider" title="Plegar/expandir">▾</button>
      </div>
      <div class="field-frame-slider-track">${dotsHtml}</div>
    </div>
  `;

  attachToggleHandler(containerEl);
  attachLayerSelectHandler(containerEl, onLayerChange);

  // Delegación de eventos en el track.
  const track = containerEl.querySelector('.field-frame-slider-track');
  if (track && typeof onSelect === 'function') {
    track.addEventListener(
      'click',
      (ev) => {
        const target = ev.target instanceof Element ? ev.target : null;
        if (!target) return;
        const btn = target.closest('.field-frame-dot');
        if (!btn || !track.contains(btn)) return;
        const observed = btn.getAttribute('data-observed');
        if (!observed) return;
        const frame = list.find((f) => sameDate(f?.observed_at, observed));
        if (frame) {
          try {
            onSelect(frame);
          } catch (err) {
            // No rompemos el render por un handler que lance.
            // eslint-disable-next-line no-console
            console.warn('fieldFrameSlider onSelect error:', err);
          }
        }
      },
      { passive: true },
    );
  }

  // Scroll para que el dot activo sea visible.
  const activeEl = containerEl.querySelector('.field-frame-dot.active');
  if (activeEl && typeof activeEl.scrollIntoView === 'function') {
    try {
      activeEl.scrollIntoView({
        behavior: 'auto',
        block: 'nearest',
        inline: 'center',
      });
    } catch (_err) {
      // Algunos navegadores antiguos no soportan options; ignoramos.
    }
  }
}
