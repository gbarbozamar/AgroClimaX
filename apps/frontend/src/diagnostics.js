/**
 * AgroClimaX — Módulo de diagnóstico.
 *
 * Captura console logs, errores JS, unhandledrejections, requests HTTP,
 * eventos custom y acciones de usuario en un buffer circular. Expone una
 * API pública para que la sidebar rendere un resumen y el usuario pueda
 * copiar todo al clipboard / descargarlo como JSON / enviar al backend.
 *
 * Uso:
 *   import { diagnostics, initDiagnostics } from './diagnostics.js';
 *   initDiagnostics();
 *   diagnostics.track('layer_toggle', { layerId: 'rgb', enabled: true });
 */
import { store } from './state.js?v=20260420-3';

const MAX_ENTRIES = 500;
const LS_BUFFER_KEY = 'agroclimax.diag.buffer';
const LS_ENABLED_KEY = 'agroclimax.diag.enabled';
const LS_PERSIST_LIMIT = 100;  // cantidad que se guarda a localStorage (el resto solo memoria)

const state = {
  entries: [],
  startedAt: new Date().toISOString(),
  enabled: true,
  listeners: new Set(),
  nextId: 1,
};

function readLSBool(key, fallback) {
  try {
    const raw = window.localStorage.getItem(key);
    if (raw === null) return fallback;
    return raw === '1' || raw === 'true';
  } catch (_) {
    return fallback;
  }
}

function writeLSBool(key, value) {
  try {
    window.localStorage.setItem(key, value ? '1' : '0');
  } catch (_) {
    // noop
  }
}

function safeStringify(value, maxLen = 4000) {
  try {
    if (value === undefined) return undefined;
    if (typeof value === 'string') return value.length > maxLen ? `${value.slice(0, maxLen)}...[truncated]` : value;
    const seen = new WeakSet();
    const replacer = (_key, val) => {
      if (val instanceof Error) {
        return { _errorName: val.name, _message: val.message, _stack: val.stack?.split('\n').slice(0, 5).join(' | ') };
      }
      if (val && typeof val === 'object') {
        if (seen.has(val)) return '[circular]';
        seen.add(val);
      }
      if (typeof val === 'function') return `[fn ${val.name || 'anon'}]`;
      return val;
    };
    const out = JSON.stringify(value, replacer);
    return out && out.length > maxLen ? `${out.slice(0, maxLen)}...[truncated]` : out;
  } catch (_) {
    return '[unserializable]';
  }
}

function notify() {
  state.listeners.forEach((fn) => {
    try { fn(); } catch (_) { /* noop */ }
  });
}

function persistSnapshot() {
  if (!state.enabled) return;
  try {
    const tail = state.entries.slice(-LS_PERSIST_LIMIT);
    window.localStorage.setItem(LS_BUFFER_KEY, JSON.stringify(tail));
  } catch (_) {
    // noop; probablemente storage lleno
  }
}

function push(level, type, message, meta) {
  if (!state.enabled) return;
  const entry = {
    id: state.nextId++,
    t: new Date().toISOString(),
    level,
    type,
    message: typeof message === 'string' ? message : safeStringify(message),
    meta: meta === undefined ? undefined : safeStringify(meta),
  };
  state.entries.push(entry);
  if (state.entries.length > MAX_ENTRIES) {
    state.entries.splice(0, state.entries.length - MAX_ENTRIES);
  }
  persistSnapshot();
  notify();
}

/* ---------------- Console wrapper ---------------- */

const originalConsole = {
  log: console.log.bind(console),
  info: console.info.bind(console),
  warn: console.warn.bind(console),
  error: console.error.bind(console),
};

function installConsoleHooks() {
  ['log', 'info', 'warn', 'error'].forEach((method) => {
    const original = originalConsole[method];
    console[method] = (...args) => {
      try {
        const message = args.map((a) => {
          if (a instanceof Error) return `${a.name}: ${a.message}`;
          return typeof a === 'string' ? a : safeStringify(a, 400);
        }).join(' ');
        push(method === 'log' ? 'info' : method, 'console', message);
      } catch (_) { /* swallow; no romper console */ }
      try { original(...args); } catch (_) { /* noop */ }
    };
  });
}

/* ---------------- Error hooks ---------------- */

function installErrorHooks() {
  window.addEventListener('error', (event) => {
    const err = event.error || event;
    push('error', 'window_error', err?.message || 'Error sin mensaje', {
      filename: event.filename,
      lineno: event.lineno,
      colno: event.colno,
      stack: err?.stack?.split('\n').slice(0, 5).join(' | '),
    });
  });
  window.addEventListener('unhandledrejection', (event) => {
    const reason = event.reason;
    push('error', 'unhandled_rejection', reason?.message || safeStringify(reason, 400), {
      stack: reason?.stack?.split('\n').slice(0, 5).join(' | '),
    });
  });
}

/* ---------------- CustomEvent listeners ---------------- */

function installCustomEventHooks() {
  const events = [
    'agroclimax:unauthorized',
    'agroclimax:timeline-date-change',
    'agroclimax:viewport-preload-started',
  ];
  events.forEach((name) => {
    window.addEventListener(name, (event) => {
      push('info', 'custom_event', name, event?.detail ?? null);
    });
  });
}

/* ---------------- Fetch hook ---------------- */

export function recordFetch({ url, method, status, ok, durationMs, error, preview }) {
  push(ok ? 'info' : 'error', 'fetch', `${method || 'GET'} ${url} -> ${status ?? 'network_error'} (${Math.round(durationMs)}ms)`, {
    status,
    ok,
    durationMs,
    error,
    preview: preview ? safeStringify(preview, 800) : undefined,
  });
}

/* ---------------- API pública ---------------- */

export const diagnostics = {
  /** Agregar una entrada manual. */
  log(level, message, meta) {
    push(level || 'info', 'manual', message, meta);
  },
  /** Trackear una acción de usuario con detalle opcional. */
  track(action, detail) {
    push('action', 'user_action', action, detail);
  },
  /** Retorna snapshot JSON completo. */
  snapshot() {
    return {
      startedAt: state.startedAt,
      generatedAt: new Date().toISOString(),
      enabled: state.enabled,
      userAgent: navigator.userAgent,
      viewport: { w: window.innerWidth, h: window.innerHeight, dpr: window.devicePixelRatio },
      location: window.location.href,
      storeSnapshot: buildStoreSnapshot(),
      entries: [...state.entries],
      counts: {
        total: state.entries.length,
        errors: state.entries.filter((e) => e.level === 'error').length,
        fetches: state.entries.filter((e) => e.type === 'fetch').length,
        actions: state.entries.filter((e) => e.type === 'user_action').length,
      },
    };
  },
  /** Copia snapshot al portapapeles. */
  async copy() {
    const json = JSON.stringify(this.snapshot(), null, 2);
    try {
      await navigator.clipboard.writeText(json);
      push('info', 'manual', 'Diagnóstico copiado al portapapeles', { size: json.length });
      return { ok: true, size: json.length };
    } catch (err) {
      push('error', 'manual', 'Fallo al copiar al portapapeles', { error: err?.message });
      return { ok: false, error: err?.message || String(err) };
    }
  },
  /** Descarga snapshot como archivo JSON. */
  download() {
    const json = JSON.stringify(this.snapshot(), null, 2);
    const blob = new Blob([json], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    const ts = new Date().toISOString().replace(/[:.]/g, '-');
    a.download = `agroclimax-diag-${ts}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    push('info', 'manual', 'Diagnóstico descargado', { filename: a.download });
  },
  /** Envía snapshot al backend (silencioso). */
  async sendToBackend() {
    const payload = this.snapshot();
    try {
      const res = await fetch('/api/v1/client-diagnostics', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      push(res.ok ? 'info' : 'warn', 'manual', `Diagnóstico enviado al backend: ${res.status}`);
      return { ok: res.ok, status: res.status };
    } catch (err) {
      push('error', 'manual', 'No se pudo enviar el diagnóstico al backend', { error: err?.message });
      return { ok: false, error: err?.message };
    }
  },
  /** Resetea el buffer. */
  clear() {
    state.entries.length = 0;
    state.nextId = 1;
    state.startedAt = new Date().toISOString();
    try { window.localStorage.removeItem(LS_BUFFER_KEY); } catch (_) { /* noop */ }
    notify();
  },
  /** Lectura rápida del buffer. */
  entries() {
    return [...state.entries];
  },
  /** Conteos rápidos (para UI). */
  stats() {
    return {
      total: state.entries.length,
      errors: state.entries.filter((e) => e.level === 'error').length,
      warns: state.entries.filter((e) => e.level === 'warn').length,
      fetches: state.entries.filter((e) => e.type === 'fetch').length,
      actions: state.entries.filter((e) => e.type === 'user_action').length,
      startedAt: state.startedAt,
    };
  },
  setEnabled(value) {
    state.enabled = Boolean(value);
    writeLSBool(LS_ENABLED_KEY, state.enabled);
    notify();
  },
  isEnabled() {
    return state.enabled;
  },
  subscribe(fn) {
    state.listeners.add(fn);
    return () => state.listeners.delete(fn);
  },
};

/* ---------------- Store snapshot ---------------- */

const STORE_KEYS = [
  'selectedScope', 'selectedDepartment', 'selectedUnitId',
  'selectedFieldId', 'selectedPaddockId', 'selectedEstablishmentId',
  'selectedSectionId', 'selectedProductiveId', 'selectedHexId',
  'currentLayer', 'activeLayers', 'timelineDate', 'timelineEnabled',
  'timelineForecastCollapsed', 'preloadStatus', 'preloadRunKey',
  'authUser', 'authReady', 'profileStatus',
];

function buildStoreSnapshot() {
  const out = {};
  STORE_KEYS.forEach((k) => {
    const v = store[k];
    if (v === undefined || v === null) { out[k] = v; return; }
    if (Array.isArray(v)) { out[k] = v.slice(0, 20); return; }
    if (typeof v === 'object') {
      // user object: solo ID + email + full_name
      if (k === 'authUser') { out[k] = { id: v.id, email: v.email, full_name: v.full_name }; return; }
      out[k] = safeStringify(v, 600);
      return;
    }
    out[k] = v;
  });
  return out;
}

/* ---------------- Init ---------------- */

export function initDiagnostics() {
  state.enabled = readLSBool(LS_ENABLED_KEY, true);
  // Restaurar últimas entradas del localStorage para ver historial post-reload
  try {
    const raw = window.localStorage.getItem(LS_BUFFER_KEY);
    if (raw) {
      const prev = JSON.parse(raw);
      if (Array.isArray(prev)) {
        state.entries.push(...prev);
        state.nextId = (prev[prev.length - 1]?.id || 0) + 1;
      }
    }
  } catch (_) { /* noop */ }

  installConsoleHooks();
  installErrorHooks();
  installCustomEventHooks();
  push('info', 'manual', 'Diagnóstico inicializado', { persistedEntries: state.entries.length });
  window.agroDiagnostics = diagnostics;  // acceso desde consola para debug manual
}
